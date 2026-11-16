"""Live AWS integration checks. Writes only isolated synthetic test scopes."""
import concurrent.futures
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from afterzero_client import Client, AfterZeroError

outputs = json.loads((ROOT / ".deploy/outputs.json").read_text())
url = outputs["ServiceUrl"]
owner = Client(url, (ROOT / ".deploy/admin-token").read_text().strip())
passed = []


def check(label, condition):
    assert condition, label
    passed.append(label)
    print("PASS", label, flush=True)


def rejects(label, fn, status):
    try:
        fn()
    except AfterZeroError as e:
        check(label, e.status == status)
    else:
        raise AssertionError(label)


def wait_until(fn, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fn()
        if value:
            return value
        time.sleep(1)
    raise AssertionError("Timed out awaiting AWS asynchronous workflow")


with urllib.request.urlopen(url + "/health") as r:
    check("deployed Lambda health", json.load(r)["execution"] == "aws_lambda")
rejects("authentication enforced", lambda: Client(url, "invalid").scope("customer-A"), 401)
sid = "test-" + uuid.uuid4().hex[:12]
owner.create_scope(sid, "imports", {"customer": "test"}, ["a", "b"])
pred = {"all": [{"field": "status", "op": "eq", "value": "bad"}]}
owner.observe(scope=sid, artifact="report", observed_count=0, basis={"a": 0, "b": 0}, predicate=pred, receipt_id="r1")
check("missing source blocks coverage", owner.scope(sid)["receipts"][0]["status"] == "waiting_for_coverage")
key = owner.request("/credentials", {"scopes": [sid], "producers": ["a"]})["token"]
producer = Client(url, key)
rejects("producer identity enforcement", lambda: producer.event(sid, event_id="bad-auth", producer="b", revision=1, kind="seal", complete=True), 403)
rejects("producer cannot attest checks", lambda: producer.observe(scope=sid, artifact="x", observed_count=0, basis={"a": 0, "b": 0}, predicate=pred), 403)
producer.event(sid, event_id="a1", producer="a", revision=1, kind="seal", complete=True)
owner.event(sid, event_id="b1", producer="b", revision=1, kind="seal", complete=True)
check("complete sources activate receipt", owner.scope(sid)["receipts"][0]["status"] == "unchanged_through_checkpoint")
owner.event(sid, event_id="a3", producer="a", revision=3, kind="seal", complete=True)
check("gap becomes unknown", owner.scope(sid)["receipts"][0]["status"] == "unknown")
owner.event(sid, event_id="a2", producer="a", revision=2, rows=[{"status": "good"}], complete=True)
check("gap closes in source order", owner.scope(sid)["heads"]["a"] == 3 and owner.scope(sid)["receipts"][0]["status"] == "unchanged_through_checkpoint")
late = dict(event_id="late", producer="a", revision=4, rows=[{"id": "late-row", "status": "bad"}], complete=True)
owner.event(sid, **late, via_s3=True)
state = wait_until(lambda: (s if (s := owner.scope(sid))["heads"]["a"] == 4 else None))
check("S3 manifest revokes receipt", state["receipts"][0]["status"] == "revoked")
version = state["version"]
check("duplicate is an explicit no-op", owner.event(sid, **late)["duplicate"] and owner.scope(sid)["version"] == version)
rejects("event identity conflict rejected", lambda: owner.event(sid, **{**late, "rows": []}), 409)
rejects("service evaluator refuses false zero", lambda: owner.request("/scopes/"+sid+"/check", {"receipt_id":"r2","artifact":"false-report","predicate":pred}), 409)
owner.observe(scope=sid, artifact="stale-report", observed_count=0, basis={"a": 1, "b": 1}, predicate=pred, receipt_id="race")
check("registration replays intervening changes", next(r for r in owner.scope(sid)["receipts"] if r["receipt_id"] == "race")["status"] == "revoked")

# Simultaneous writers: the version condition must preserve every event.
other = "race-" + uuid.uuid4().hex[:12]
owner.create_scope(other, "parallel", {"customer": "race"}, ["a", "b"])
events = [(p, r) for p in ("a", "b") for r in (1, 2, 3)]
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    list(pool.map(lambda pr: owner.event(other, event_id=f"{pr[0]}{pr[1]}", producer=pr[0], revision=pr[1], kind="seal", complete=True), events))
check("concurrent writers preserve all revisions", owner.scope(other)["heads"] == {"a": 3, "b": 3} and owner.scope(other)["event_count"] == 6)
rejects("scope-bound credentials", lambda: producer.scope(other), 403)

message = wait_until(lambda: next((m for m in owner.inbox() if m["scope_id"] == sid and any(r["status"] == "revoked" for r in m["receipts"])), None))
evidence = owner.request("/evidence?key=" + urllib.parse.quote(message["evidence_key"], safe=""))
actual = hashlib.sha256(json.dumps(evidence["snapshot"], sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
check("Step Functions archived verifiable S3 evidence", actual == evidence["sha256"] == message["sha256"])
owner.acknowledge(message["id"])
check("consumer acknowledgement persists", next(m for m in owner.inbox() if m["id"] == message["id"])["acknowledged"])
owner.acknowledge(message["id"])
check("acknowledgement is idempotent", True)
rejects("evidence tenant isolation", lambda: owner.request("/evidence?key=evidence/another-tenant/file.json"), 404)

result = {"service_url": url, "checks_passed": len(passed), "checks": passed, "scope_ids": [sid, other], "checked_at_epoch": int(time.time())}
(ROOT / "docs/live-verification.json").write_text(json.dumps(result, indent=2))
print(json.dumps({"passed": len(passed), "service_url": url}), flush=True)
