"""Dependency-free Python client. Preserve request IDs when retrying mutations."""
import json
import urllib.error
import urllib.parse
import urllib.request
import uuid


class AfterZeroError(Exception):
    def __init__(self, status, message):
        self.status = status
        super().__init__(message)


class Client:
    def __init__(self, base_url, token):
        if not base_url.startswith("https://"):
            raise ValueError("Use an HTTPS service URL")
        self.base_url, self.token = base_url.rstrip("/"), token

    def request(self, path, body=None, method=None):
        request = urllib.request.Request(self.base_url + "/api" + path,
                  data=json.dumps(body).encode() if body is not None else None,
                  method=method or ("POST" if body is not None else "GET"),
                  headers={"Content-Type": "application/json", "Authorization": "Bearer " + self.token})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as e:
            try:
                message = json.load(e).get("error", e.reason)
            except (ValueError, AttributeError):
                message = str(e.reason)
            raise AfterZeroError(e.code, message) from None

    def create_scope(self, scope_id, dataset, partition, required_producers, lifetime_seconds=86400):
        return self.request("/scopes", locals_without_self(locals()))

    def scope(self, scope_id):
        return self.request("/scopes/" + urllib.parse.quote(scope_id, safe=""))

    def event(self, scope_id, *, event_id, producer, revision, kind="append", rows=None, complete=False, via_s3=False):
        body = {"event_id": event_id, "producer": producer, "revision": revision, "kind": kind,
                "rows": rows or [], "complete": complete}
        return self.request("/scopes/" + scope_id + ("/manifests" if via_s3 else "/events"), body)

    def observe(self, *, scope, artifact, observed_count, basis, predicate, receipt_id=None, valid_for_seconds=86400):
        return self.request("/scopes/" + scope + "/observations", {
            "receipt_id": receipt_id or "r_" + uuid.uuid4().hex,
            "artifact": artifact, "observed_count": observed_count, "basis": basis,
            "predicate": predicate, "valid_for_seconds": valid_for_seconds})

    def inbox(self):
        return self.request("/inbox")["messages"]

    def acknowledge(self, message_id):
        return self.request("/ack", {"id": message_id})


def locals_without_self(values):
    return {k: v for k, v in values.items() if k != "self"}
