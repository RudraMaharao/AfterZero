"""An existing SQL job integrating with the real deployed AfterZero API.

AFTERZERO_URL and AFTERZERO_TOKEN are supplied by the caller, never committed.
This fixture uses SQLite and synthetic data. A real producer must durably couple
data publication to its revision manifest/outbox rather than make a best-effort
HTTP call after a database commit.
"""
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from afterzero_client import Client

client = Client(os.environ["AFTERZERO_URL"], os.environ["AFTERZERO_TOKEN"])
scope = "sql-" + uuid.uuid4().hex[:12]
client.create_scope(scope, "customer_imports", {"customer": "integration-example", "business_date": "2026-11-13"}, ["sql-job"])
db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE imports (id TEXT, validation_status TEXT, business_date TEXT)")
db.execute("INSERT INTO imports VALUES ('initial', 'valid', '2026-11-13')")
client.event(scope, event_id="initial", producer="sql-job", revision=1, rows=[{"id": "initial", "validation_status": "valid"}], complete=True)
count = db.execute("SELECT count(*) FROM imports WHERE business_date = ? AND validation_status = ?", ("2026-11-13", "invalid")).fetchone()[0]
registered = client.observe(scope=scope, artifact="sql-report-v1", observed_count=count, basis={"sql-job": 1},
                            predicate={"all": [{"field": "validation_status", "op": "eq", "value": "invalid"}]}, receipt_id="sql-receipt")
assert registered["scope"]["receipts"][0]["status"] == "unchanged_through_checkpoint"
print("SQL check returned", count, "failures; receipt is covered.")
db.execute("INSERT INTO imports VALUES ('late', 'invalid', '2026-11-13')")
client.event(scope, event_id="late", producer="sql-job", revision=2, rows=[{"id": "late", "validation_status": "invalid"}], complete=True, via_s3=True)
for _ in range(30):
    receipt = client.scope(scope)["receipts"][0]
    if receipt["status"] == "revoked":
        break
    time.sleep(1)
assert receipt["status"] == "revoked", receipt
print("S3 change manifest revoked the receipt:", json.dumps(receipt["reason"]))
print("Scope:", scope)
