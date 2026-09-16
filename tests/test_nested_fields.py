"""Tests for the nested-struct field helpers in lakeflow_declarative_pipeline.py.

These cover the functions added to handle nested Salesforce objects such as
`BillingAddress.Street`: _path_key, _path_sql, _leaf_specs,
_schema_leaf_paths, _build_struct_touched_sql, and _build_merge_sql
(including the available_paths schema-evolution guard).

Uses the same AST-extraction approach as test_unified_diff.py to load pure
functions without importing the full pipeline module (which requires
pyspark.pipelines, only available inside a Databricks runtime).

Run standalone:  python tests/test_nested_fields.py
Or with pytest:  pytest tests/test_nested_fields.py
"""

import ast
import pathlib
import re

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

_SRC = pathlib.Path(__file__).resolve().parent.parent / "lakeflow_declarative_pipeline.py"


def _load_nested_functions():
    """Exec only the nested-field helpers from the pipeline source."""
    tree = ast.parse(_SRC.read_text())
    wanted = {
        "_path_key",
        "_path_sql",
        "_leaf_specs",
        "_schema_leaf_paths",
        "_build_struct_touched_sql",
        "_build_merge_sql",
    }
    keep = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    namespace = {
        "re": re,
        "isinstance": isinstance,
        "StructType": StructType,
        "StringType": StringType,
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), str(_SRC), "exec"), namespace)
    return namespace


_NS = _load_nested_functions()
_path_key = _NS["_path_key"]
_path_sql = _NS["_path_sql"]
_leaf_specs = _NS["_leaf_specs"]
_schema_leaf_paths = _NS["_schema_leaf_paths"]
_build_struct_touched_sql_raw = _NS["_build_struct_touched_sql"]
_build_merge_sql_raw = _NS["_build_merge_sql"]


def _build_struct_touched_sql(path, data_type, available_paths=None):
    """Wrapper: defaults available_paths to all leaves under the struct."""
    if available_paths is None:
        available_paths = {p for p, _ in _leaf_specs(data_type.fields, path)}
    return _build_struct_touched_sql_raw(path, data_type, available_paths)


def _build_merge_sql(path, data_type, mode, available_paths=None):
    """Wrapper: defaults available_paths to all leaves of the type."""
    if available_paths is None:
        if isinstance(data_type, StructType):
            available_paths = {p for p, _ in _leaf_specs(data_type.fields, path)}
        else:
            available_paths = {path}
    return _build_merge_sql_raw(path, data_type, mode, available_paths)


# ---------------------------------------------------------------------------
# Sample schemas mirroring Salesforce nested structs
# ---------------------------------------------------------------------------

_FLAT_FIELDS = [
    StructField("FirstName", StringType()),
    StructField("LastName", StringType()),
    StructField("Age", LongType()),
]

_BILLING_ADDRESS_TYPE = StructType([
    StructField("Street", StringType()),
    StructField("City", StringType()),
    StructField("State", StringType()),
    StructField("Latitude", DoubleType()),
])

_NESTED_FIELDS = [
    StructField("Name", StringType()),
    StructField("BillingAddress", _BILLING_ADDRESS_TYPE),
    StructField("Revenue", DoubleType()),
]

# Doubly nested: Address.Geo.Lat / Address.Geo.Lng
_GEO_TYPE = StructType([
    StructField("Lat", DoubleType()),
    StructField("Lng", DoubleType()),
])
_DEEP_ADDRESS_TYPE = StructType([
    StructField("Street", StringType()),
    StructField("Geo", _GEO_TYPE),
])
_DEEP_FIELDS = [
    StructField("Id", StringType()),
    StructField("Address", _DEEP_ADDRESS_TYPE),
]


# ===========================================================================
# _path_key
# ===========================================================================

def test_path_key_flat():
    assert _path_key("FirstName") == "FirstName"


def test_path_key_nested():
    assert _path_key("BillingAddress.Street") == "BillingAddress_Street"


def test_path_key_deeply_nested():
    assert _path_key("Address.Geo.Lat") == "Address_Geo_Lat"


# ===========================================================================
# _path_sql
# ===========================================================================

def test_path_sql_flat():
    assert _path_sql("Name", "t") == "t.`Name`"


def test_path_sql_nested():
    assert _path_sql("BillingAddress.Street", "t") == "t.`BillingAddress`.`Street`"


def test_path_sql_nested_alias_s():
    assert _path_sql("BillingAddress.City", "s") == "s.`BillingAddress`.`City`"


def test_path_sql_deeply_nested():
    assert _path_sql("Address.Geo.Lat", "t") == "t.`Address`.`Geo`.`Lat`"


# ===========================================================================
# _leaf_specs
# ===========================================================================

def test_leaf_specs_flat_fields():
    specs = _leaf_specs(_FLAT_FIELDS)
    paths = [s[0] for s in specs]
    assert paths == ["FirstName", "LastName", "Age"]


def test_leaf_specs_nested_struct():
    specs = _leaf_specs(_NESTED_FIELDS)
    paths = [s[0] for s in specs]
    # Name (flat) + 4 BillingAddress leaves + Revenue (flat) = 6
    assert len(specs) == 6
    assert "Name" in paths
    assert "BillingAddress.Street" in paths
    assert "BillingAddress.City" in paths
    assert "BillingAddress.State" in paths
    assert "BillingAddress.Latitude" in paths
    assert "Revenue" in paths


def test_leaf_specs_preserves_types():
    spec_dict = dict(_leaf_specs(_NESTED_FIELDS))
    assert isinstance(spec_dict["Name"], StringType)
    assert isinstance(spec_dict["BillingAddress.Street"], StringType)
    assert isinstance(spec_dict["BillingAddress.Latitude"], DoubleType)
    assert isinstance(spec_dict["Revenue"], DoubleType)


def test_leaf_specs_deeply_nested():
    specs = _leaf_specs(_DEEP_FIELDS)
    paths = [s[0] for s in specs]
    assert "Address.Street" in paths
    assert "Address.Geo.Lat" in paths
    assert "Address.Geo.Lng" in paths
    # Struct nodes themselves are NOT leaves
    assert "Address" not in paths
    assert "Address.Geo" not in paths


def test_leaf_specs_empty():
    assert _leaf_specs([]) == []


def test_leaf_specs_struct_only_field():
    """A list with only a struct field -> only the leaves of that struct."""
    fields = [StructField("Addr", StructType([StructField("Zip", StringType())]))]
    specs = _leaf_specs(fields)
    assert len(specs) == 1
    assert specs[0][0] == "Addr.Zip"


# ===========================================================================
# _schema_leaf_paths
# ===========================================================================

def test_schema_leaf_paths_flat():
    schema = StructType(_FLAT_FIELDS)
    paths = _schema_leaf_paths(schema)
    assert paths == {"FirstName", "LastName", "Age"}


def test_schema_leaf_paths_nested():
    schema = StructType(_NESTED_FIELDS)
    paths = _schema_leaf_paths(schema)
    assert "Name" in paths
    assert "BillingAddress.Street" in paths
    assert "BillingAddress.Latitude" in paths
    assert "Revenue" in paths
    # Struct nodes are NOT leaf paths
    assert "BillingAddress" not in paths


def test_schema_leaf_paths_deeply_nested():
    schema = StructType(_DEEP_FIELDS)
    paths = _schema_leaf_paths(schema)
    assert "Address.Street" in paths
    assert "Address.Geo.Lat" in paths
    assert "Address.Geo.Lng" in paths
    assert "Address" not in paths
    assert "Address.Geo" not in paths


def test_schema_leaf_paths_empty():
    assert _schema_leaf_paths(StructType([])) == set()


# ===========================================================================
# _build_struct_touched_sql (all paths available)
# ===========================================================================

def test_struct_touched_sql_flat_struct():
    """A struct with only flat leaves -> OR of __chg__ flags."""
    simple = StructType([
        StructField("Street", StringType()),
        StructField("City", StringType()),
    ])
    result = _build_struct_touched_sql("Addr", simple)
    assert "s.`__chg__Addr_Street`" in result
    assert "s.`__chg__Addr_City`" in result
    assert " OR " in result


def test_struct_touched_sql_nested():
    """A struct containing a sub-struct -> recursive predicate."""
    result = _build_struct_touched_sql("Address", _DEEP_ADDRESS_TYPE)
    assert "s.`__chg__Address_Street`" in result
    assert "s.`__chg__Address_Geo_Lat`" in result
    assert "s.`__chg__Address_Geo_Lng`" in result


def test_struct_touched_sql_single_leaf():
    """A struct with one leaf -> no OR, just the flag."""
    single = StructType([StructField("Zip", LongType())])
    result = _build_struct_touched_sql("Addr", single)
    assert result == "(s.`__chg__Addr_Zip`)"
    assert " OR " not in result


# ===========================================================================
# available_paths guard — _build_struct_touched_sql
# ===========================================================================

def test_struct_touched_partial_paths():
    """Only leaves present in available_paths contribute to the predicate."""
    simple = StructType([
        StructField("Street", StringType()),
        StructField("City", StringType()),
    ])
    # Only Street is available — City should be excluded
    result = _build_struct_touched_sql("Addr", simple, {"Addr.Street"})
    assert "s.`__chg__Addr_Street`" in result
    assert "Addr_City" not in result
    assert " OR " not in result  # single clause, no OR


def test_struct_touched_no_paths_available():
    """When no leaves are available, predicate is 'false'."""
    simple = StructType([
        StructField("Street", StringType()),
        StructField("City", StringType()),
    ])
    result = _build_struct_touched_sql("Addr", simple, set())
    assert result == "false"


def test_struct_touched_nested_partial():
    """Only the Geo sub-struct leaves are available, not Street."""
    result = _build_struct_touched_sql(
        "Address", _DEEP_ADDRESS_TYPE,
        {"Address.Geo.Lat", "Address.Geo.Lng"},  # no Address.Street
    )
    assert "Address_Geo_Lat" in result
    assert "Address_Geo_Lng" in result
    assert "Address_Street" not in result


# ===========================================================================
# _build_merge_sql — update mode (all paths available)
# ===========================================================================

def test_merge_sql_scalar_update():
    """Non-string scalar: CASE WHEN __chg__ THEN __val__ ELSE t.col END."""
    sql = _build_merge_sql("Revenue", DoubleType(), "update")
    assert "s.`__chg__Revenue`" in sql
    assert "s.`__val__Revenue`" in sql
    assert "t.`Revenue`" in sql  # fallback to target


def test_merge_sql_string_update():
    """String field: uses resolve_chain with t.col as base."""
    sql = _build_merge_sql("Name", StringType(), "update")
    assert "resolve_chain" in sql
    assert "s.`__chain__Name`" in sql
    assert "t.`Name`" in sql  # base for chain folding


def test_merge_sql_nested_struct_update():
    """Nested struct: named_struct wrapping recursive child expressions."""
    sql = _build_merge_sql("BillingAddress", _BILLING_ADDRESS_TYPE, "update")
    assert "named_struct" in sql
    # Every child field name appears as a struct key
    for child in ("Street", "City", "State", "Latitude"):
        assert f"\'{child}\'" in sql, f"missing struct key for {child}"
    # String children use resolve_chain
    assert "resolve_chain" in sql
    assert "s.`__chain__BillingAddress_Street`" in sql
    # Numeric child uses __val__
    assert "s.`__val__BillingAddress_Latitude`" in sql
    # Fallback is the target struct
    assert "t.`BillingAddress`" in sql


def test_merge_sql_deeply_nested_update():
    """Two-level nesting: Address.Geo.Lat produces nested named_struct."""
    sql = _build_merge_sql("Address", _DEEP_ADDRESS_TYPE, "update")
    assert "named_struct" in sql
    assert "'Street'" in sql
    assert "'Geo'" in sql
    assert "'Lat'" in sql
    assert "'Lng'" in sql
    assert "s.`__val__Address_Geo_Lat`" in sql
    assert "s.`__val__Address_Geo_Lng`" in sql
    # String leaf at first level uses resolve_chain
    assert "s.`__chain__Address_Street`" in sql


# ===========================================================================
# _build_merge_sql — insert mode (all paths available)
# ===========================================================================

def test_merge_sql_scalar_insert():
    """Insert mode: fallback is NULL, not t.col."""
    sql = _build_merge_sql("Revenue", DoubleType(), "insert")
    assert "ELSE NULL END" in sql
    assert "t.`Revenue`" not in sql


def test_merge_sql_string_insert():
    """Insert mode for string: base is CAST(NULL AS STRING)."""
    sql = _build_merge_sql("Name", StringType(), "insert")
    assert "CAST(NULL AS STRING)" in sql
    assert "t.`Name`" not in sql


def test_merge_sql_nested_struct_insert():
    """Insert mode: struct fallback is NULL, string base is CAST(NULL AS STRING)."""
    sql = _build_merge_sql("BillingAddress", _BILLING_ADDRESS_TYPE, "insert")
    # Top-level struct fallback
    assert "ELSE NULL END" in sql
    # String children use NULL base
    assert "CAST(NULL AS STRING)" in sql
    # Should NOT reference the target table anywhere
    assert "t.`BillingAddress`" not in sql
    assert "t.`BillingAddress`.`Street`" not in sql


# ===========================================================================
# available_paths guard — _build_merge_sql
# ===========================================================================

def test_merge_sql_missing_scalar_fallback_update():
    """Scalar leaf absent from available_paths -> fallback to target value."""
    sql = _build_merge_sql("Revenue", DoubleType(), "update", set())
    assert sql == "t.`Revenue`"


def test_merge_sql_missing_scalar_fallback_insert():
    """Scalar leaf absent from available_paths -> fallback to NULL."""
    sql = _build_merge_sql("Revenue", DoubleType(), "insert", set())
    assert sql == "NULL"


def test_merge_sql_missing_string_fallback_update():
    """String leaf absent from available_paths -> fallback to target value."""
    sql = _build_merge_sql("Name", StringType(), "update", set())
    assert sql == "t.`Name`"


def test_merge_sql_missing_string_fallback_insert():
    """String leaf absent from available_paths -> NULL."""
    sql = _build_merge_sql("Name", StringType(), "insert", set())
    assert sql == "NULL"


def test_merge_sql_struct_partial_children_update():
    """Struct with only some children available — missing ones fall back to target."""
    partial = {"BillingAddress.Street"}
    sql = _build_merge_sql("BillingAddress", _BILLING_ADDRESS_TYPE, "update", partial)
    assert "named_struct" in sql
    # Street is available -> uses resolve_chain
    assert "resolve_chain" in sql
    assert "s.`__chain__BillingAddress_Street`" in sql
    # City is a string but missing -> falls back to target
    assert "t.`BillingAddress`.`City`" in sql
    # Latitude is a scalar missing -> falls back to target
    assert "t.`BillingAddress`.`Latitude`" in sql


def test_merge_sql_struct_partial_children_insert():
    """Struct insert with only some children -> missing ones are NULL."""
    partial = {"BillingAddress.Street"}
    sql = _build_merge_sql("BillingAddress", _BILLING_ADDRESS_TYPE, "insert", partial)
    assert "named_struct" in sql
    assert "resolve_chain" in sql  # Street is available
    # Missing children should NOT reference the target table
    assert "t.`BillingAddress`.`City`" not in sql
    assert "t.`BillingAddress`.`Latitude`" not in sql


def test_merge_sql_struct_no_children_available_update():
    """Struct with zero available children -> entire struct falls back to target."""
    sql = _build_merge_sql("BillingAddress", _BILLING_ADDRESS_TYPE, "update", set())
    # The outer ELSE fallback is the whole struct from the target
    assert "t.`BillingAddress`" in sql


def test_merge_sql_deeply_nested_partial():
    """Two-level nesting: only Address.Street available, not Geo children."""
    partial = {"Address.Street"}
    sql = _build_merge_sql("Address", _DEEP_ADDRESS_TYPE, "update", partial)
    assert "named_struct" in sql
    # Street is available -> resolve_chain
    assert "s.`__chain__Address_Street`" in sql
    # Geo.Lat and Geo.Lng are missing -> fall back to target
    assert "t.`Address`.`Geo`.`Lat`" in sql
    assert "t.`Address`.`Geo`.`Lng`" in sql


# ===========================================================================
# End-to-end: BillingAddress.Street scenario
# ===========================================================================

def test_billing_address_street_end_to_end():
    """Simulate the full path for a BillingAddress.Street update.

    Verifies that the helper chain produces correct leaf paths, safe keys,
    SQL references, and MERGE expressions for the canonical Salesforce
    nested-address use case.
    """
    # 1. _leaf_specs flattens BillingAddress into leaf paths
    fields = [StructField("BillingAddress", _BILLING_ADDRESS_TYPE)]
    specs = _leaf_specs(fields)
    paths = [s[0] for s in specs]
    assert "BillingAddress.Street" in paths
    assert "BillingAddress.Latitude" in paths

    # 2. _path_key produces safe column suffixes
    assert _path_key("BillingAddress.Street") == "BillingAddress_Street"

    # 3. _path_sql produces backtick-quoted nested references
    assert _path_sql("BillingAddress.Street", "t") == "t.`BillingAddress`.`Street`"

    # 4. _build_merge_sql for the parent struct produces a named_struct
    #    that individually handles each child, including resolve_chain for Street
    update_sql = _build_merge_sql("BillingAddress", _BILLING_ADDRESS_TYPE, "update")
    assert "named_struct" in update_sql
    assert "resolve_chain(t.`BillingAddress`.`Street`" in update_sql
    assert "s.`__val__BillingAddress_Latitude`" in update_sql

    # 5. Insert mode uses NULL bases, not target references
    insert_sql = _build_merge_sql("BillingAddress", _BILLING_ADDRESS_TYPE, "insert")
    assert "CAST(NULL AS STRING)" in insert_sql
    assert "t.`BillingAddress`" not in insert_sql


def test_schema_evolution_end_to_end():
    """End-to-end schema evolution: target has BillingAddress, batch only has Street.

    The target table's schema includes BillingAddress with Street, City, State,
    Latitude. The current batch only has BillingAddress.Street (schema evolved,
    other children were removed). The MERGE should fall back to the target for
    missing children and still process Street correctly.
    """
    batch_paths = {"BillingAddress.Street"}
    update_sql = _build_merge_sql(
        "BillingAddress", _BILLING_ADDRESS_TYPE, "update", batch_paths
    )
    # Street is present -> resolve_chain
    assert "resolve_chain" in update_sql
    assert "s.`__chain__BillingAddress_Street`" in update_sql
    # City, State, Latitude are absent -> fall back to target
    assert "t.`BillingAddress`.`City`" in update_sql
    assert "t.`BillingAddress`.`State`" in update_sql
    assert "t.`BillingAddress`.`Latitude`" in update_sql

    insert_sql = _build_merge_sql(
        "BillingAddress", _BILLING_ADDRESS_TYPE, "insert", batch_paths
    )
    # Street present -> resolve_chain with NULL base
    assert "CAST(NULL AS STRING)" in insert_sql
    # Missing children -> NULL
    assert "t.`BillingAddress`.`City`" not in insert_sql


# ===========================================================================
# runner
# ===========================================================================

if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failures = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failures += 1
            print(f"FAIL {t.__name__}: {e!r}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    raise SystemExit(1 if failures else 0)
