"""AfterZero's deterministic, bounded per-scope state machine. No AWS dependency."""
import copy
import hashlib
import json
import re

VERSION = "1.0.0"
MAX_EVENTS = 128
MAX_RECEIPTS = 32


class Problem(Exception):
    def __init__(self, message, status=422):
        self.message, self.status = message, status
        super().__init__(message)


def canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError):
        raise Problem("Values must be finite JSON values")


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def ident(value, label="identifier"):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", value):
        raise Problem(f"Invalid {label}; use 1–100 letters, digits, dot, dash, colon or underscore")
    return value


def integer(value, label, minimum=0, maximum=1_000_000):
    if type(value) is not int or not minimum <= value <= maximum:
        raise Problem(f"{label} must be an integer from {minimum} to {maximum}")
    return value


def scalar(value):
    if value is not None and type(value) not in (str, int, bool):
        raise Problem("Predicate values support strings, integers, booleans and null; floats are unsupported")
    if isinstance(value, str) and len(value) > 256:
        raise Problem("Value exceeds 256 characters")
    return value


def predicate(value):
    if not isinstance(value, dict) or set(value) != {"all"}:
        raise Problem("Predicate must contain an 'all' array")
    terms = value["all"]
    if not isinstance(terms, list) or not 1 <= len(terms) <= 8:
        raise Problem("Use 1–8 predicate terms")
    for t in terms:
        if not isinstance(t, dict) or set(t) != {"field", "op", "value"}:
            raise Problem("Each term needs field, op and value")
        ident(t["field"], "field")
        if t["op"] not in ("eq", "in", "gte", "lt"):
            raise Problem("Supported operators: eq, in, gte, lt")
        if t["op"] == "in":
            if not isinstance(t["value"], list) or not 1 <= len(t["value"]) <= 32:
                raise Problem("in requires 1–32 scalar values")
            for v in t["value"]:
                scalar(v)
        else:
            scalar(t["value"])
        if t["op"] in ("gte", "lt") and type(t["value"]) is not int:
            raise Problem("Range operators use integer values (use epoch seconds for timestamps)")
    return copy.deepcopy(value)


def matches(row, pred):
    """Missing fields never match; explicit null equals null; bool != integer."""
    for term in pred["all"]:
        if term["field"] not in row:
            return False
        left, right, op = row[term["field"]], term["value"], term["op"]
        if op == "eq" and not (type(left) is type(right) and left == right):
            return False
        if op == "in" and not any(type(left) is type(v) and left == v for v in right):
            return False
        if op in ("gte", "lt"):
            if type(left) is not int or not (left >= right if op == "gte" else left < right):
                return False
    return True


def new_scope(scope_id, tenant, body, now):
    dataset = ident(body.get("dataset"), "dataset")
    partition = body.get("partition")
    if not isinstance(partition, dict) or not 1 <= len(partition) <= 8:
        raise Problem("partition must contain 1–8 named values")
    for k, v in partition.items():
        ident(k, "partition key")
        scalar(v)
    producers = body.get("required_producers")
    if not isinstance(producers, list) or not 1 <= len(producers) <= 8:
        raise Problem("Declare 1–8 required producers")
    for p in producers:
        ident(p, "producer")
    if len(set(producers)) != len(producers):
        raise Problem("Duplicate producers")
    life = integer(body.get("lifetime_seconds", 86400), "lifetime_seconds", 60, 604800)
    return {"id": scope_id, "tenant": tenant, "dataset": dataset, "partition": partition,
            "required_producers": sorted(producers), "created_at": now, "expires_at": now + life,
            "heads": {p: 0 for p in producers}, "complete": {p: False for p in producers},
            "commits": [], "pending": {}, "event_ids": {}, "receipts": {}, "version": 0}


def normalize_event(body, scope):
    if not isinstance(body, dict):
        raise Problem("Event must be an object")
    event_id = ident(body.get("event_id"), "event_id")
    producer = ident(body.get("producer"), "producer")
    if producer not in scope["heads"]:
        raise Problem("Producer is not declared for this scope")
    revision = integer(body.get("revision"), "revision", 1)
    kind = body.get("kind", "append")
    if kind not in ("append", "changed", "seal", "reopen"):
        raise Problem("kind must be append, changed, seal or reopen")
    rows = body.get("rows", [])
    if not isinstance(rows, list) or len(rows) > 25:
        raise Problem("Use at most 25 projected rows per event, or kind=changed for a coarse invalidation")
    if kind != "append" and rows:
        raise Problem("Only append events may contain rows")
    for row in rows:
        if not isinstance(row, dict) or not 1 <= len(row) <= 16:
            raise Problem("Each projected row must have 1–16 fields")
        for k, v in row.items():
            ident(k, "row field")
            scalar(v)
    if "complete" in body and type(body["complete"]) is not bool:
        raise Problem("complete must be a boolean")
    complete = body.get("complete", kind == "seal")
    if kind == "reopen":
        complete = False
    event = {"event_id": event_id, "producer": producer, "revision": revision, "kind": kind,
             "rows": copy.deepcopy(rows), "complete": complete}
    if len(canonical(event).encode()) > 12000:
        raise Problem("Projected event exceeds 12KB; send a coarse changed event instead", 413)
    return event


def ingest(scope, body, now):
    event = normalize_event(body, scope)
    fp = digest(event)
    existing = scope["event_ids"].get(event["event_id"])
    if existing:
        if existing != fp:
            raise Problem("event_id already exists with different content", 409)
        return scope, {"duplicate": True, "event_id": event["event_id"]}
    if now >= scope["expires_at"]:
        raise Problem("Scope expired; create a new scope", 409)
    if len(scope["event_ids"]) >= MAX_EVENTS:
        raise Problem("Scope event limit reached; rotate to a new partition/scope", 409)
    p, rev = event["producer"], event["revision"]
    slot = f"{p}:{rev}"
    if rev <= scope["heads"][p] or slot in scope["pending"]:
        raise Problem("Revision already occupied by a different event", 409)
    if rev > scope["heads"][p] + 32:
        raise Problem("Revision gap exceeds 32; reconcile the source first", 409)
    result = copy.deepcopy(scope)
    result["event_ids"][event["event_id"]] = fp
    result["pending"][slot] = event
    while f"{p}:{result['heads'][p] + 1}" in result["pending"]:
        applied = result["pending"].pop(f"{p}:{result['heads'][p] + 1}")
        applied["applied_at"] = now
        result["commits"].append(applied)
        result["heads"][p] = applied["revision"]
        result["complete"][p] = applied["complete"]
    return result, {"duplicate": False, "event_id": event["event_id"], "buffered": slot in result["pending"]}


def observe(scope, body, now):
    if not isinstance(body, dict):
        raise Problem("Observation must be an object")
    rid = ident(body.get("receipt_id"), "receipt_id")
    if body.get("observed_count") != 0 or type(body.get("observed_count")) is not int:
        raise Problem("Only a successfully evaluated zero-result observation can be registered")
    pred = predicate(body.get("predicate"))
    basis = body.get("basis")
    if not isinstance(basis, dict) or set(basis) != set(scope["heads"]):
        raise Problem("basis must give a revision for every required producer")
    for p, r in basis.items():
        integer(r, "basis revision")
        if r > scope["heads"][p]:
            raise Problem("Observation references a revision not yet covered by this service", 409)
    artifact = ident(body.get("artifact"), "artifact")
    seconds = integer(body.get("valid_for_seconds", 86400), "valid_for_seconds", 1, 604800)
    request = {"receipt_id": rid, "artifact": artifact, "observed_count": 0,
               "predicate": pred, "basis": copy.deepcopy(basis), "valid_for_seconds": seconds}
    old = scope["receipts"].get(rid)
    if old:
        if old["request_hash"] != digest(request):
            raise Problem("Receipt ID already used for a different observation", 409)
        return scope, {"duplicate": True, "receipt_id": rid}
    if now >= scope["expires_at"]:
        raise Problem("Scope expired", 409)
    if len(scope["receipts"]) >= MAX_RECEIPTS:
        raise Problem("Scope receipt limit reached", 409)
    result = copy.deepcopy(scope)
    result["receipts"][rid] = {**request, "created_at": now,
                              "expires_at": min(now + seconds, scope["expires_at"]),
                              "request_hash": digest(request), "evidence_mode": "producer_attested"}
    return result, {"duplicate": False, "receipt_id": rid}


def receipt_status(scope, receipt, now):
    relevant = [e for e in scope["commits"] if e["revision"] > receipt["basis"][e["producer"]]]
    reason = None
    for event in relevant:
        if event["kind"] == "append":
            for row in event["rows"]:
                if matches(row, receipt["predicate"]):
                    reason = {"code": "matching_late_record", "event_id": event["event_id"],
                              "producer": event["producer"], "revision": event["revision"], "witness": row}
                    break
        if event["kind"] == "reopen":
            reason = {"code": "coverage_withdrawn", "event_id": event["event_id"], "producer": event["producer"]}
        if reason:
            break
    if reason:
        status = "revoked"
    elif now >= receipt["expires_at"]:
        status, reason = "expired", {"code": "receipt_expired"}
    elif scope["pending"]:
        status, reason = "unknown", {"code": "revision_gap", "pending": sorted(scope["pending"])}
    elif any(e["kind"] == "changed" for e in relevant):
        status, reason = "recheck_required", {"code": "coarse_source_change"}
    elif not all(scope["complete"].values()):
        status, reason = "waiting_for_coverage", {"code": "incomplete_producers", "producers": [p for p, v in scope["complete"].items() if not v]}
    else:
        status, reason = "unchanged_through_checkpoint", {"code": "covered", "detail": "Conditional on producer assertions; no claim about unseen revisions"}
    return {**receipt, "status": status, "reason": reason, "covered_through": scope["heads"], "evaluated_at": now}


def view(scope, now):
    return {k: v for k, v in {**scope,
            "receipts": [receipt_status(scope, r, now) for r in scope["receipts"].values()],
            "pending_count": len(scope["pending"]), "event_count": len(scope["event_ids"])}.items()
            if k not in ("event_ids", "pending", "tenant")}


def evaluate(scope, pred):
    """Actual bounded reference evaluator for append-only event projections.

    The generic observation API accepts attestations. This evaluator independently
    computes a result over stored projections, and refuses incomplete histories.
    """
    pred = predicate(pred)
    if scope["pending"]:
        raise Problem("Cannot evaluate while source revisions have gaps", 409)
    if any(e["kind"] == "changed" for e in scope["commits"]):
        raise Problem("Coarse changes require reevaluation in the original source", 409)
    rows = [row for e in scope["commits"] if e["kind"] == "append" for row in e["rows"]]
    failing = [r for r in rows if matches(r, pred)]
    return {"observed_count": len(failing), "rows_evaluated": len(rows), "failing_rows": failing[:10], "basis": scope["heads"], "predicate": pred}
