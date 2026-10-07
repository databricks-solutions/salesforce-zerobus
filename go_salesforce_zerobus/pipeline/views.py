"""Typed views over salesforce_current, built from Salesforce Avro schemas.

Pure Python (no Spark): generate_views.py reads the Lakebase schema registry
and runs the SQL built here; tests run locally.

Each (org, object) view has one column per field the object has ever had in
that org (the union of its schema versions, so a removed field stays as a
NULL column and readers never break). Types come from the Avro schema:

- Avro primitives map to SQL types; nested records (Address, Name) become
  STRUCTs; Salesforce's `doc` hints turn DateTime fields (epoch millis) into
  TIMESTAMPs and Date fields into DATEs.
- Custom fields carry no type hint (their `doc` is the field ID), so a custom
  date or datetime stays BIGINT epoch millis unless overridden.
- Values are read with try_variant_get, so a value that doesn't fit its type is
  NULL rather than a query error.
"""

import hashlib
import json
import re

HEADER = "ChangeEventHeader"

AVRO_SQL = {
    "string": "string",
    "boolean": "boolean",
    "int": "int",
    "long": "bigint",
    "float": "float",
    "double": "double",
    "bytes": "string",  # JSON carries bytes as text
    "enum": "string",
    "fixed": "string",
}


def slug(s):
    """A lower-case SQL identifier fragment."""
    return re.sub(r"[^a-z0-9_]+", "_", (s or "").lower()).strip("_") or "x"


def view_name(tenant_key, org_id, entity_name):
    return f"{slug(tenant_key or org_id)}__{slug(entity_name)}"


def fingerprint(schema_ids):
    """Stable fingerprint of an (org, object)'s schema versions."""
    return hashlib.sha1("\n".join(sorted(set(schema_ids))).encode()).hexdigest()


def _sql_str(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _quote(ident):
    return "`" + ident.replace("`", "``") + "`"


def _unwrap(t):
    """Drop null from a union."""
    if isinstance(t, list):
        rest = [x for x in t if x != "null"]
        return rest[0] if len(rest) == 1 else "string"
    return t


def sql_type(avro_type, named=None):
    """SQL type for an Avro type. named resolves references to named types
    defined earlier in the schema (e.g. a second Address field)."""
    named = {} if named is None else named
    t = _unwrap(avro_type)
    if isinstance(t, str):
        if t in AVRO_SQL:
            return AVRO_SQL[t]
        return named.get(t, "string")
    kind = t.get("type")
    if kind == "record":
        inner = ", ".join(f"{_quote(f['name'])}: {sql_type(f['type'], named)}" for f in t.get("fields", []))
        named[t.get("name", "")] = f"struct<{inner}>"
        return named[t.get("name", "")]
    if kind == "array":
        return f"array<{sql_type(t.get('items'), named)}>"
    if kind == "map":
        return f"map<string, {sql_type(t.get('values'), named)}>"
    if kind in ("enum", "fixed"):
        named[t.get("name", "")] = "string"
        return "string"
    if isinstance(kind, (str, list, dict)):
        return sql_type(kind, named)
    return "string"


def column_expr(name, avro_type, doc, named, override=None):
    """SELECT expression for one top-level field of `fields`."""
    path = _sql_str("$." + name)
    hint = (doc or "").rsplit(":", 1)[-1] if (doc or "").startswith(("Data:", "ForeignKey:")) else ""
    kind = override or {"DateTime": "timestamp", "Date": "date"}.get(hint)
    if kind == "timestamp":
        expr = f"timestamp_millis(try_variant_get(fields, {path}, 'bigint'))"
    elif kind == "date":
        # Accept days since epoch or epoch millis.
        v = f"try_variant_get(fields, {path}, 'bigint')"
        expr = f"CASE WHEN abs({v}) < 100000000 THEN date_from_unix_date(CAST({v} AS INT)) ELSE to_date(timestamp_millis({v})) END"
    else:
        expr = f"try_variant_get(fields, {path}, {_sql_str(override or sql_type(avro_type, named))})"
    return f"{expr} AS {_quote(name)}"


def fields_of(schema_json):
    """(name, avro type, doc) for each business field, in schema order."""
    schema = json.loads(schema_json)
    return [(f["name"], f["type"], f.get("doc")) for f in schema.get("fields", []) if f["name"] != HEADER]


def union_fields(schemas):
    """Fields across schema versions (newest first): the newest version's
    order and type win; fields only in older versions are appended."""
    seen, out = set(), []
    for schema_json in schemas:
        for name, typ, doc in fields_of(schema_json):
            if name not in seen:
                seen.add(name)
                out.append((name, typ, doc))
    return out


def select_list(fields, overrides=None):
    named = {}
    overrides = overrides or {}
    cols = []
    for name, typ, doc in fields:
        cols.append(column_expr(name, typ, doc, named, overrides.get(name)))
    return cols


def org_view_sql(fqn, current_table, org_id, entity_name, schemas, overrides=None, comment=None):
    """CREATE OR REPLACE VIEW for one org's object."""
    cols = ["org_id", "Id"] + select_list(union_fields(schemas), overrides) + ["_last_change_type", "_updated_at"]
    body = ",\n  ".join(cols)
    comment = comment or f"{entity_name} records for org {org_id}, typed from its Salesforce schema. Generated; do not edit."
    return (
        f"CREATE OR REPLACE VIEW {fqn}\nCOMMENT {_sql_str(comment)}\nAS SELECT\n  {body}\n"
        f"FROM {current_table}\nWHERE org_id = {_sql_str(org_id)} AND entity_name = {_sql_str(entity_name)} AND NOT _is_deleted"
    )


def common_fields(latest_by_org):
    """Fields present with the same SQL type in every org's latest schema."""
    if not latest_by_org:
        return []
    per_org = []
    for schema_json in latest_by_org:
        named = {}
        per_org.append({name: (sql_type(typ, named), typ, doc) for name, typ, doc in fields_of(schema_json)})
    first = fields_of(latest_by_org[0])
    out = []
    for name, typ, doc in first:
        types = {m.get(name, (None,))[0] for m in per_org}
        if len(types) == 1 and None not in types:
            out.append((name, typ, doc))
    return out


def object_view_sql(fqn, current_table, entity_name, latest_by_org, overrides=None):
    """CREATE OR REPLACE VIEW of one object across orgs, with the fields every
    org shares (custom fields that differ per org stay in `fields`)."""
    cols = ["org_id", "Id"] + select_list(common_fields(latest_by_org), overrides) + ["fields", "_last_change_type", "_updated_at"]
    body = ",\n  ".join(cols)
    comment = f"{entity_name} records across orgs: fields shared by every org, plus `fields` for the rest. Generated; do not edit."
    return (
        f"CREATE OR REPLACE VIEW {fqn}\nCOMMENT {_sql_str(comment)}\nAS SELECT\n  {body}\n"
        f"FROM {current_table}\nWHERE entity_name = {_sql_str(entity_name)} AND NOT _is_deleted"
    )
