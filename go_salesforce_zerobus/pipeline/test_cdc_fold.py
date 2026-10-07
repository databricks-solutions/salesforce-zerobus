"""Tests for cdc_fold: run with `pytest pipeline/` (no Spark needed)."""

import hashlib

import cdc_fold
from cdc_fold import fold, seq_key


def ev(change_type, n, record=None, changed=(), nulled=(), diff=(), commit_ts=None):
    """An event at position n (commit timestamp n unless given)."""
    return {
        "change_type": change_type,
        "record": record or {},
        "changed_fields": list(changed),
        "nulled_fields": list(nulled),
        "diff_fields": list(diff),
        "seq": seq_key(commit_ts if commit_ts is not None else n, n, 0, f"{n:04x}"),
    }


ACCOUNT = {
    "ChangeEventHeader": {"entityName": "Account", "changeType": "CREATE"},
    "Name": "Acme",
    "Type": None,
    "BillingAddress": {"Street": "1 Main", "City": "SF", "State": None},
    "Region__c": "West",
}


def test_create_replaces_state_without_header_or_nulls():
    out = fold(None, False, None, [ev("CREATE", 1, ACCOUNT)])
    assert out["fields"] == {"Name": "Acme", "BillingAddress": {"Street": "1 Main", "City": "SF"}, "Region__c": "West"}
    assert out["deleted"] is False and out["change_type"] == "CREATE"


def test_sparse_update_keeps_untouched_fields_and_compound_siblings():
    created = fold(None, False, None, [ev("CREATE", 1, ACCOUNT)])
    update = ev("UPDATE", 2,
                {"Name": None, "BillingAddress": {"Street": None, "City": "Oakland", "State": None}, "Region__c": "East"},
                changed=["BillingAddress", "BillingAddress.City", "Region__c"])
    out = fold(created["fields"], False, created["seq"], [update])
    assert out["fields"] == {"Name": "Acme", "BillingAddress": {"Street": "1 Main", "City": "Oakland"}, "Region__c": "East"}


def test_nulled_fields_are_removed_and_empty_compounds_pruned():
    state = {"Name": "Acme", "BillingAddress": {"City": "SF"}, "Region__c": "West"}
    update = ev("UPDATE", 2, {}, changed=["Region__c", "BillingAddress", "BillingAddress.City"],
                nulled=["Region__c", "BillingAddress.City"])
    out = fold(state, False, None, [update])
    assert out["fields"] == {"Name": "Acme"}


def test_update_without_children_replaces_the_whole_field():
    state = {"Tags__c": {"a": 1}}
    out = fold(state, False, None, [ev("UPDATE", 2, {"Tags__c": {"b": 2}}, changed=["Tags__c"])])
    assert out["fields"] == {"Tags__c": {"b": 2}}


def test_diff_field_is_applied_to_the_prior_value():
    prev = "line1\nline2\nline3"
    new = "line1\nLINE2\nline3"
    digest = hashlib.sha256(new.encode()).hexdigest()
    diff = "\n".join(["--- old", f"+++ {digest}", "@@ -1,3 +1,3 @@", " line1", "-line2", "+LINE2", " line3"])
    out = fold({"Description": prev}, False, None,
               [ev("UPDATE", 2, {"Description": diff}, changed=["Description"], diff=["Description"])])
    assert out["fields"]["Description"] == new

    # A diff that doesn't reproduce the hash keeps the last good value.
    bad = diff.replace(digest, "0" * 64)
    out = fold({"Description": prev}, False, None,
               [ev("UPDATE", 3, {"Description": bad}, changed=["Description"], diff=["Description"])])
    assert out["fields"]["Description"] == prev


def test_events_apply_in_order_within_a_batch():
    events = [
        ev("UPDATE", 3, {"Name": "Third"}, changed=["Name"]),
        ev("CREATE", 1, {"Name": "First", "Region__c": "West"}),
        ev("UPDATE", 2, {"Name": "Second"}, changed=["Name"]),
    ]
    out = fold(None, False, None, events)
    assert out["fields"] == {"Name": "Third", "Region__c": "West"}
    assert out["seq"] == seq_key(3, 3, 0, "0003")


def test_watermark_skips_redelivered_and_older_events():
    state = {"Name": "Current"}
    watermark = seq_key(5, 5, 0, "0005")
    stale = [ev("UPDATE", 4, {"Name": "Old"}, changed=["Name"]), ev("UPDATE", 5, {"Name": "Dup"}, changed=["Name"])]
    assert fold(state, False, watermark, stale) is None
    out = fold(state, False, watermark, stale + [ev("UPDATE", 6, {"Name": "New"}, changed=["Name"])])
    assert out["fields"] == {"Name": "New"}


def test_delete_keeps_fields_and_undelete_restores():
    created = fold(None, False, None, [ev("CREATE", 1, ACCOUNT)])
    deleted = fold(created["fields"], False, created["seq"], [ev("DELETE", 2)])
    assert deleted["deleted"] is True and deleted["fields"]["Name"] == "Acme"
    # A redelivered update from before the delete can't resurrect the record.
    assert fold(deleted["fields"], True, deleted["seq"], [ev("UPDATE", 1, {"Name": "X"}, changed=["Name"])]) is None
    restored = fold(deleted["fields"], True, deleted["seq"], [ev("UNDELETE", 3, {"Name": "Acme", "Region__c": "North"})])
    assert restored["deleted"] is False and restored["fields"] == {"Name": "Acme", "Region__c": "North"}


def test_gap_events_change_nothing():
    out = fold({"Name": "Acme"}, False, None, [ev("GAP_UPDATE", 2)])
    assert out["fields"] == {"Name": "Acme"} and out["change_type"] == "GAP_UPDATE"


def test_prior_state_is_not_mutated():
    state = {"BillingAddress": {"City": "SF"}}
    fold(state, False, None, [ev("UPDATE", 2, {"BillingAddress": {"City": "LA"}}, changed=["BillingAddress", "BillingAddress.City"])])
    assert state == {"BillingAddress": {"City": "SF"}}


def test_json_round_trip():
    fields = {"Name": "Ünïcode", "AnnualRevenue": 1234.5, "NumberOfEmployees": 12}
    assert cdc_fold.loads(cdc_fold.dumps(fields)) == fields
    assert cdc_fold.loads(None) is None
