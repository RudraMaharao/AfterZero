import copy
import itertools
import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from core import Problem, evaluate, ingest, matches, new_scope, observe, predicate, receipt_status

PRED = {"all": [{"field": "status", "op": "eq", "value": "bad"}]}


def scope():
    return new_scope("scope", "tenant", {"dataset": "imports", "partition": {"customer": "A"}, "required_producers": ["a", "b"]}, 100)


def event(p, r, rows=None, kind="append", complete=True):
    return {"event_id": f"{p}-{r}", "producer": p, "revision": r, "rows": rows or [], "kind": kind, "complete": complete}


def observation(s, basis=None, rid="receipt", lifetime=1000):
    return {"receipt_id": rid, "artifact": "report", "observed_count": 0, "predicate": PRED,
            "basis": basis if basis is not None else copy.deepcopy(s["heads"]), "valid_for_seconds": lifetime}


def apply(s, e):
    return ingest(s, e, 101)[0]


def status(s, at=102):
    return receipt_status(s, s["receipts"]["receipt"], at)["status"]


def registered():
    s = scope()
    return observe(s, observation(s), 100)[0]


def test_zero_without_coverage_is_not_green():
    s = registered()
    assert status(s) == "waiting_for_coverage"
    s = apply(s, event("a", 1))
    assert status(s) == "waiting_for_coverage"
    s = apply(s, event("b", 1))
    assert status(s) == "unchanged_through_checkpoint"


def test_late_row_that_never_existed_revokes():
    s = apply(apply(registered(), event("a", 1)), event("b", 1))
    s = apply(s, event("a", 2, [{"id": "late", "status": "bad"}]))
    assert status(s) == "revoked"
    assert receipt_status(s, s["receipts"]["receipt"], 103)["reason"]["witness"]["id"] == "late"


def test_registration_race_replays_changes_since_basis():
    s = apply(scope(), event("a", 1, [{"status": "bad"}]))
    s = observe(s, observation(s, {"a": 0, "b": 0}), 101)[0]
    assert status(s) == "revoked"


def test_future_basis_is_rejected():
    s = scope()
    with pytest.raises(Problem, match="not yet covered"):
        observe(s, observation(s, {"a": 1, "b": 0}), 100)


def test_idempotent_event_and_conflicting_replay():
    s = apply(registered(), event("a", 1))
    result, meta = ingest(s, event("a", 1), 101)
    assert result is s and meta["duplicate"]
    with pytest.raises(Problem, match="different content"):
        ingest(s, event("a", 1, [{"status": "bad"}]), 101)


def test_revision_collision_even_with_new_event_id():
    s = apply(scope(), event("a", 1))
    body = event("a", 1)
    body["event_id"] = "different"
    with pytest.raises(Problem, match="occupied"):
        ingest(s, body, 101)


def test_gap_is_unknown_then_resolves_in_order():
    s = apply(registered(), event("a", 2))
    assert s["heads"]["a"] == 0 and status(s) == "unknown"
    s = apply(s, event("a", 1))
    assert s["heads"]["a"] == 2
    s = apply(s, event("b", 1))
    assert status(s) == "unchanged_through_checkpoint"


def test_late_bad_event_buffered_until_gap_filled():
    s = apply(registered(), event("a", 2, [{"status": "bad"}]))
    assert status(s) == "unknown"
    s = apply(s, event("a", 1))
    assert status(s) == "revoked"


def test_all_delivery_orders_have_same_final_result():
    events = [event("a", 1), event("a", 2, [{"status": "bad"}]), event("b", 1)]
    for order in itertools.permutations(events):
        s = registered()
        for e in order:
            s = apply(s, e)
        assert status(s) == "revoked"
        assert s["heads"] == {"a": 2, "b": 1}


def test_coarse_change_requires_recheck_not_false_revocation():
    s = apply(registered(), event("a", 1, kind="changed"))
    assert status(s) == "recheck_required"
    with pytest.raises(Problem, match="original source"):
        evaluate(s, PRED)


def test_withdrawn_completeness_is_terminal_for_old_receipt():
    s = apply(registered(), event("a", 1, kind="reopen"))
    s = apply(s, event("a", 2, kind="seal"))
    assert status(s) == "revoked"


def test_expiry_enforced_at_read_without_cleanup_worker():
    s = scope()
    s = observe(s, observation(s, lifetime=10), 100)[0]
    assert status(s, 110) == "expired"


def test_scope_isolation():
    a, b = registered(), registered()
    original = copy.deepcopy(b)
    a = apply(a, event("a", 1, [{"status": "bad"}]))
    assert status(a) == "revoked" and b == original


def test_pure_transition_does_not_mutate_input():
    s = registered()
    before = copy.deepcopy(s)
    apply(s, event("a", 1))
    assert s == before


def test_observation_replay_is_idempotent():
    s = scope()
    body = observation(s)
    s, _ = observe(s, body, 100)
    result, meta = observe(s, body, 200)
    assert meta["duplicate"] and result is s
    assert s["receipts"]["receipt"]["created_at"] == 100


def test_receipt_id_cannot_be_reassigned():
    s = registered()
    body = observation(s)
    body["artifact"] = "another"
    with pytest.raises(Problem, match="different observation"):
        observe(s, body, 101)


@pytest.mark.parametrize("count", [1, -1, False, "0", None])
def test_non_zero_or_untyped_observation_rejected(count):
    s = scope()
    body = observation(s)
    body["observed_count"] = count
    with pytest.raises(Problem):
        observe(s, body, 100)


def test_missing_and_null_are_distinct_and_bool_not_int():
    p = predicate({"all": [{"field": "x", "op": "eq", "value": None}]})
    assert matches({"x": None}, p) and not matches({}, p)
    p = predicate({"all": [{"field": "x", "op": "eq", "value": 1}]})
    assert matches({"x": 1}, p) and not matches({"x": True}, p)


def test_half_open_integer_range():
    p = predicate({"all": [{"field": "t", "op": "gte", "value": 10}, {"field": "t", "op": "lt", "value": 20}]})
    assert matches({"t": 10}, p) and not matches({"t": 20}, p) and not matches({"t": "15"}, p)


def test_unsupported_predicate_fails_closed():
    with pytest.raises(Problem):
        predicate({"all": [{"field": "x", "op": "regex", "value": ".*"}]})
    with pytest.raises(Problem):
        predicate({"all": [{"field": "x", "op": "lt", "value": "2026-01-01"}]})


def test_reference_evaluator_finds_real_failure():
    s = apply(scope(), event("a", 1, [{"status": "valid"}, {"status": "bad"}]))
    result = evaluate(s, PRED)
    assert result["rows_evaluated"] == 2 and result["observed_count"] == 1


def test_randomized_append_oracle_and_duplicate_delivery():
    for seed in range(100):
        rng = random.Random(seed)
        s = registered()
        rows = [{"status": rng.choice(["valid", "valid", "bad"])} for _ in range(8)]
        events = [event("a", i + 1, [r]) for i, r in enumerate(rows)] + [event("b", 1)]
        order = events + rng.sample(events, 4)
        rng.shuffle(order)
        for e in order:
            s = apply(s, e)
        expected = "revoked" if any(r["status"] == "bad" for r in rows) else "unchanged_through_checkpoint"
        assert status(s) == expected
        assert evaluate(s, PRED)["observed_count"] == sum(r["status"] == "bad" for r in rows)
