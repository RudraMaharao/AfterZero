"""AWS HTTP API, durable per-scope transactions, and manifest ingestion."""
import base64
import hashlib
import hmac
import json
import mimetypes
import os
from pathlib import Path
import secrets
import time
import urllib.parse

import boto3
from botocore.exceptions import ClientError
from core import Problem, canonical, digest, evaluate, ident, ingest, new_scope, observe, view

TABLE = os.environ.get("TABLE_NAME", "afterzero")
BUCKET = os.environ.get("BUCKET_NAME", "")
table = boto3.resource("dynamodb").Table(TABLE)
s3 = boto3.client("s3")
WEB = Path(__file__).parent / "web"


def now():
    return int(time.time())


def load_scope(tenant, sid):
    ident(sid, "scope_id")
    item = table.get_item(Key={"pk": tenant, "sk": f"SCOPE#{sid}"}, ConsistentRead=True).get("Item")
    if not item:
        raise Problem("Scope not found", 404)
    return json.loads(item["state"])


def transact_scope(tenant, sid, fn):
    for _ in range(8):
        before = load_scope(tenant, sid)
        after, result = fn(before)
        if result.get("duplicate"):
            return before, result
        after["version"] = before["version"] + 1
        save_scope(after, expected=before["version"])
        return after, result
    raise Problem("Concurrent updates; retry this request", 409)


def mutate(tenant, sid, fn):
    for _ in range(8):
        try:
            return transact_scope(tenant, sid, fn)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
    raise Problem("Concurrent updates; retry with the same request identifiers", 409)


def save_scope(state, expected=None):
    encoded = canonical(state)
    if len(encoded.encode()) > 170000:
        raise Problem("Scope storage limit reached; use smaller scopes or coarse change notifications", 413)
    snapshot = view(state, now())
    effect = {"tenant": state["tenant"], "scope_id": state["id"], "version": state["version"],
              "evaluated_at": now(), "heads": state["heads"], "complete": state["complete"],
              "receipts": snapshot["receipts"], "event_count": snapshot["event_count"],
              "pending_count": snapshot["pending_count"], "partition": state["partition"], "dataset": state["dataset"]}
    args = {"Item": {"pk": state["tenant"], "sk": f"SCOPE#{state['id']}", "state": encoded,
                     "version": state["version"], "effect": canonical(effect), "ttl": state["expires_at"] + 86400}}
    if expected is None:
        args["ConditionExpression"] = "attribute_not_exists(pk)"
    else:
        args.update(ConditionExpression="#v = :v", ExpressionAttributeNames={"#v": "version"}, ExpressionAttributeValues={":v": expected})
    table.put_item(**args)


def authorize(headers):
    raw = headers.get("authorization", "")
    if not raw.startswith("Bearer "):
        raise Problem("Bearer token required. Create a demo session or use your deployment token.", 401)
    token = raw[7:]
    admin = os.environ.get("ADMIN_TOKEN", "")
    if admin and hmac.compare_digest(token, admin):
        return "owner", False, None
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    session = table.get_item(Key={"pk": "SESSION", "sk": token_hash}, ConsistentRead=True).get("Item")
    if not session or int(session["ttl"]) <= now():
        raise Problem("Session expired or invalid", 401)
    return session["tenant"], session.get("kind", "demo") == "demo", json.loads(session["permissions"]) if "permissions" in session else None


def list_scopes(tenant):
    result = table.query(KeyConditionExpression="pk = :p AND begins_with(sk, :s)",
                         ExpressionAttributeValues={":p": tenant, ":s": "SCOPE#"}, ConsistentRead=True, Limit=50)
    return [view(json.loads(i["state"]), now()) for i in result.get("Items", [])]


def create_scope(tenant, body):
    sid = ident(body.get("scope_id", "sc_" + secrets.token_hex(8)), "scope_id")
    state = new_scope(sid, tenant, body, now())
    try:
        save_scope(state)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise Problem("Scope ID already exists", 409)
        raise
    return state


def new_demo(event):
    # Public playground uses a global daily cap to bound anonymous provisioning.
    day = time.strftime("%Y-%m-%d", time.gmtime())
    try:
        table.update_item(Key={"pk": "QUOTA", "sk": day},
                          UpdateExpression="SET #n = if_not_exists(#n, :z) + :one, #t = :ttl",
                          ConditionExpression="attribute_not_exists(#n) OR #n < :max",
                          ExpressionAttributeNames={"#n": "count", "#t": "ttl"},
                          ExpressionAttributeValues={":z": 0, ":one": 1, ":max": 300, ":ttl": now()+172800})
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise Problem("Public demo daily capacity reached; use a deployment token", 429)
        raise
    token, tenant = secrets.token_urlsafe(32), "demo_" + secrets.token_hex(10)
    table.put_item(Item={"pk": "SESSION", "sk": hashlib.sha256(token.encode()).hexdigest(), "tenant": tenant, "ttl": now()+3600})
    for customer in ("A", "B"):
        state = create_scope(tenant, {"scope_id": f"customer-{customer}", "dataset": "daily_imports",
                                     "partition": {"customer": customer, "business_date": day},
                                     "required_producers": ["uploader", "validator"], "lifetime_seconds": 3600})
        mutate(tenant, state["id"], lambda s: ingest(s, {"event_id": f"initial-{customer}", "producer": "uploader",
               "revision": 1, "kind": "append", "rows": [{"id": f"row-{customer}", "validation_status": "valid"}], "complete": True}, now()))
        def register_demo(s):
            pred = {"all": [{"field": "validation_status", "op": "eq", "value": "invalid"}]}
            result = evaluate(s, pred)
            updated, metadata = observe(s, {"receipt_id": f"report-{customer}", "artifact": f"daily-report-{customer}",
                    "observed_count": result["observed_count"], "predicate": pred,
                    "basis": result["basis"], "valid_for_seconds": 3500}, now())
            updated["receipts"][f"report-{customer}"]["evidence_mode"] = "service_evaluated_projection"
            return updated, metadata
        mutate(tenant, state["id"], register_demo)
    return {"token": token, "expires_in": 3600, "scopes": list_scopes(tenant), "mode": "live_aws_synthetic_inputs"}


def response(data, status=200, mime="application/json"):
    return {"statusCode": status, "headers": {"Content-Type": mime, "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer",
            "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"},
            "body": canonical(data) if mime == "application/json" else data}


def handler(event, context):
    # Async sources must raise on failure so Lambda retries / failure destinations
    # work. HTTP error responses are only meaningful for API Gateway requests.
    if event.get("Records") and event["Records"][0].get("eventSource") == "aws:s3":
        for record in event["Records"]:
            key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
            if not key.startswith("inputs/"):
                continue
            data = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            mutate(data["tenant"], data["scope_id"], lambda s: ingest(s, data["event"], now()))
        return {"ok": True}
    try:
        path = event.get("rawPath", event.get("path", "/"))
        method = event.get("requestContext", {}).get("http", {}).get("method", event.get("httpMethod", "GET"))
        headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
        if method == "GET" and path in ("/", "/app.js", "/style.css", "/architecture.svg", "/favicon.svg"):
            filename = "index.html" if path == "/" else path[1:]
            return response((WEB / filename).read_text(), mime=mimetypes.guess_type(filename)[0] or "text/plain")
        if path == "/health" and method == "GET":
            return response({"service": "afterzero", "version": "1.0.0", "storage": "dynamodb", "execution": "aws_lambda", "status": "ok"})
        raw = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            raw = base64.b64decode(raw).decode()
        if len(raw.encode()) > 48000:
            raise Problem("Request exceeds 48KB", 413)
        body = json.loads(raw)
        if not isinstance(body, dict):
            raise Problem("Request body must be a JSON object")
        if path == "/api/demo" and method == "POST":
            return response(new_demo(event), 201)
        tenant, demo, permissions = authorize(headers)
        parts = path.strip("/").split("/")
        if path == "/api/credentials" and method == "POST":
            if demo or permissions:
                raise Problem("Only the deployment owner may issue integration credentials", 403)
            scope_ids = body.get("scopes")
            producers = body.get("producers")
            if not isinstance(scope_ids, list) or not 1 <= len(scope_ids) <= 16 or not isinstance(producers, list) or not 1 <= len(producers) <= 8:
                raise Problem("Declare 1–16 scopes and 1–8 producers")
            for sid in scope_ids:
                target = load_scope(tenant, sid)
                for producer in producers:
                    if producer not in target["heads"]:
                        raise Problem("Producer not declared in every credential scope")
            from core import integer
            lifetime = integer(body.get("lifetime_seconds", 86400), "lifetime_seconds", 60, 604800)
            grant = {"scopes": scope_ids, "producers": producers, "can_observe": body.get("can_observe") is True}
            key = secrets.token_urlsafe(32)
            table.put_item(Item={"pk": "SESSION", "sk": hashlib.sha256(key.encode()).hexdigest(), "tenant": tenant,
                                "kind": "integration", "permissions": canonical(grant), "ttl": now()+lifetime})
            return response({"token": key, "expires_in": lifetime, "permissions": grant}, 201)
        if path == "/api/scopes":
            if method == "GET":
                listed = list_scopes(tenant)
                return response({"scopes": [s for s in listed if not permissions or s["id"] in permissions["scopes"]]})
            if method == "POST":
                if demo or permissions:
                    raise Problem("Public demo has two isolated scopes; deploy your own stack for custom scopes", 403)
                return response(view(create_scope(tenant, body), now()), 201)
        if len(parts) >= 3 and parts[:2] == ["api", "scopes"]:
            sid = parts[2]
            if permissions and sid not in permissions["scopes"]:
                raise Problem("Credential is not authorized for this scope", 403)
            scope = load_scope(tenant, sid)
            if len(parts) == 3 and method == "GET":
                return response(view(scope, now()))
            if len(parts) == 4 and method == "POST":
                action = parts[3]
                if permissions:
                    if action in ("events", "manifests") and body.get("producer") not in permissions["producers"]:
                        raise Problem("Credential cannot publish for this producer", 403)
                    if action in ("observations", "check") and not permissions["can_observe"]:
                        raise Problem("Credential cannot register observations", 403)
                if action == "events":
                    state, result = mutate(tenant, sid, lambda s: ingest(s, body, now()))
                    return response({**result, "scope": view(state, now())})
                if action == "observations":
                    state, result = mutate(tenant, sid, lambda s: observe(s, body, now()))
                    return response({**result, "scope": view(state, now())}, 201)
                if action == "check":
                    # Evaluation and registration share one conditional scope update.
                    pred = body.get("predicate")
                    def check(s):
                        result = evaluate(s, pred)
                        if result["observed_count"]:
                            raise Problem("Check failed: matching rows exist; a zero-result receipt cannot be issued", 409)
                        updated, registered = observe(s, {**body, "observed_count": 0, "basis": s["heads"]}, now())
                        if not registered["duplicate"]:
                            updated["receipts"][body["receipt_id"]]["evidence_mode"] = "service_evaluated_projection"
                        return updated, {**registered, "evaluation": result}
                    state, result = mutate(tenant, sid, check)
                    return response({**result, "scope": view(state, now())}, 201)
                if action == "evaluate":
                    return response(evaluate(scope, body.get("predicate")))
                if action == "manifests":
                    from core import normalize_event
                    normalized = normalize_event(body, scope)
                    key = f"inputs/{tenant}/{sid}/{digest(normalized)}.json"
                    s3.put_object(Bucket=BUCKET, Key=key, Body=canonical({"tenant": tenant, "scope_id": sid, "event": normalized}).encode(), ContentType="application/json")
                    return response({"accepted": True, "object_key": key, "event_id": normalized["event_id"], "processing": "asynchronous_s3_notification"}, 202)
        if path == "/api/inbox" and method == "GET":
            if permissions:
                raise Problem("Use a deployment owner or demo credential to access the consumer inbox", 403)
            result = table.query(KeyConditionExpression="pk = :p AND begins_with(sk, :s)", ExpressionAttributeValues={":p": tenant, ":s": "INBOX#"}, ConsistentRead=True, Limit=100, ScanIndexForward=False)
            return response({"messages": [{**json.loads(i["message"]), "id": i["sk"], "acknowledged": bool(i.get("acknowledged", False))} for i in result.get("Items", [])]})
        if path == "/api/ack" and method == "POST":
            if permissions:
                raise Problem("Consumer credential required", 403)
            mid = body.get("id", "")
            if not isinstance(mid, str) or not mid.startswith("INBOX#") or len(mid) > 200:
                raise Problem("Invalid inbox message ID")
            try:
                table.update_item(Key={"pk": tenant, "sk": mid}, UpdateExpression="SET acknowledged = :a", ConditionExpression="attribute_exists(pk)", ExpressionAttributeValues={":a": True})
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    raise Problem("Message not found", 404)
                raise
            return response({"acknowledged": True, "meaning": "Consumer acknowledged receipt; this does not prove remediation"})
        if path == "/api/evidence" and method == "GET":
            if permissions:
                raise Problem("Consumer credential required", 403)
            query = event.get("queryStringParameters") or {}
            key = query.get("key", "")
            if not key.startswith(f"evidence/{tenant}/"):
                raise Problem("Evidence not found", 404)
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            return response(json.loads(obj["Body"].read()))
        raise Problem("Endpoint not found", 404)
    except Problem as e:
        return response({"error": e.message}, e.status)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, KeyError) as e:
        return response({"error": "Malformed request; check the API schema"}, 422)
    except Exception as e:
        print(json.dumps({"error_type": type(e).__name__, "request_id": getattr(context, "aws_request_id", None)}))
        raise
