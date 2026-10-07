"""Tests for views: run with `pytest pipeline/` (no Spark needed)."""

import json

import views

ADDRESS = {"type": "record", "name": "Address", "fields": [
    {"name": "Street", "type": ["null", "string"]},
    {"name": "Latitude", "type": ["null", "double"]},
]}


def schema(*fields):
    return json.dumps({"type": "record", "name": "AccountChangeEvent", "fields": [
        {"name": "ChangeEventHeader", "type": {"type": "record", "name": "ChangeEventHeader", "fields": []}, "doc": "Data:ComplexValueType"},
        *fields,
    ]})


V1 = schema(
    {"name": "Name", "type": ["null", "string"], "doc": "Data:Switchable_PersonName"},
    {"name": "BillingAddress", "type": ["null", ADDRESS], "doc": "Data:Address"},
    {"name": "ShippingAddress", "type": ["null", "Address"], "doc": "Data:Address"},
    {"name": "CreatedDate", "type": ["null", "long"], "doc": "Data:DateTime"},
    {"name": "LastActivityDate", "type": ["null", "long"], "doc": "Data:Date"},
    {"name": "AnnualRevenue", "type": ["null", "double"], "doc": "Data:Currency"},
    {"name": "NumberOfEmployees", "type": ["null", "int"], "doc": "Data:Integer"},
    {"name": "Old__c", "type": ["null", "string"], "doc": "00N000000000001"},
)
V2 = schema(
    {"name": "Name", "type": ["null", "string"], "doc": "Data:Switchable_PersonName"},
    {"name": "Region__c", "type": ["null", "string"], "doc": "00N000000000002"},
    {"name": "SLAExpirationDate__c", "type": ["null", "long"], "doc": "00N000000000003"},
)


def test_types_and_hints():
    sql = views.org_view_sql("c.s.acme__account", "c.s.salesforce_current", "00D1", "Account", [V1])
    assert "try_variant_get(fields, '$.Name', 'string') AS `Name`" in sql
    assert "try_variant_get(fields, '$.BillingAddress', 'struct<`Street`: string, `Latitude`: double>') AS `BillingAddress`" in sql
    assert "try_variant_get(fields, '$.ShippingAddress', 'struct<`Street`: string, `Latitude`: double>') AS `ShippingAddress`" in sql
    assert "timestamp_millis(try_variant_get(fields, '$.CreatedDate', 'bigint')) AS `CreatedDate`" in sql
    assert "date_from_unix_date" in sql and "AS `LastActivityDate`" in sql
    assert "try_variant_get(fields, '$.AnnualRevenue', 'double')" in sql
    assert "try_variant_get(fields, '$.NumberOfEmployees', 'int')" in sql
    assert "ChangeEventHeader" not in sql
    assert sql.rstrip().endswith("WHERE org_id = '00D1' AND entity_name = 'Account' AND NOT _is_deleted")


def test_union_keeps_removed_fields_and_newest_order():
    names = [n for n, _, _ in views.union_fields([V2, V1])]
    assert names[:3] == ["Name", "Region__c", "SLAExpirationDate__c"]
    assert "Old__c" in names and names.count("Name") == 1


def test_custom_fields_without_hints_and_overrides():
    sql = views.org_view_sql("v", "t", "00D1", "Account", [V2])
    assert "try_variant_get(fields, '$.SLAExpirationDate__c', 'bigint')" in sql
    sql = views.org_view_sql("v", "t", "00D1", "Account", [V2], overrides={"SLAExpirationDate__c": "date"})
    assert "AS `SLAExpirationDate__c`" in sql and "date_from_unix_date" in sql


def test_object_view_has_shared_fields_only():
    sql = views.object_view_sql("c.s.all_orgs__account", "t", "Account", [V1, V2])
    assert "AS `Name`" in sql and "Region__c" not in sql and "Old__c" not in sql
    assert "  fields,\n" in sql


def test_names_and_fingerprint():
    assert views.view_name("orgfarm-dev", "00D1", "Invoice__c") == "orgfarm_dev__invoice__c"
    assert views.view_name(None, "00Dfj00000GZ4dpEAD", "Account") == "00dfj00000gz4dpead__account"
    assert views.fingerprint(["b", "a", "a"]) == views.fingerprint(["a", "b"])
    assert views.org_view_sql("v", "t", "o'x", "Account", [V2]).count("'o\\'x'") == 1
