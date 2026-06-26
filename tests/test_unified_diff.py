"""Tests for the unified-diff reconstruction in lakeflow_declarative_pipeline.py.

These cover `_parse_unified_diff` / `_rebuild_from_hunks` / `_apply_unified_diff`,
which rebuild a large text field from the unified diff Salesforce CDC sends for it.

The pipeline module imports pyspark + dlt at the top, which aren't importable outside
a Databricks runtime, so we load just the three pure functions (and the `_HUNK_RE`
module global they use) out of the source via AST instead of importing the module.

Run standalone (no pytest needed):  python tests/test_unified_diff.py
Or with pytest if available:        pytest tests/test_unified_diff.py
"""

import ast
import hashlib
import pathlib
import re

_SRC = pathlib.Path(__file__).resolve().parent.parent / "lakeflow_declarative_pipeline.py"


def _load_diff_functions():
    """Exec only the diff helpers (+ the _HUNK_RE global) from the pipeline source."""
    tree = ast.parse(_SRC.read_text())
    wanted = {"_parse_unified_diff", "_rebuild_from_hunks", "_apply_unified_diff"}
    keep = [
        node
        for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in wanted)
        or (
            isinstance(node, ast.Assign)
            and any(getattr(t, "id", None) == "_HUNK_RE" for t in node.targets)
        )
    ]
    namespace = {"re": re, "hashlib": hashlib}
    exec(compile(ast.Module(body=keep, type_ignores=[]), str(_SRC), "exec"), namespace)
    return namespace


_NS = _load_diff_functions()
apply_unified_diff = _NS["_apply_unified_diff"]


def _make_diff(new_value, hunk_header, body_lines, term="\n", bad_hash=False):
    """Build a unified-diff string whose +++ header carries sha256(new_value)."""
    digest = "deadbeef" if bad_hash else hashlib.sha256(new_value.encode("utf-8")).hexdigest()
    return "\n".join(["--- old", f"+++ {digest}", hunk_header, *body_lines])


def test_apply_add_remove_context():
    prev = "line1\nline2\nline3"
    new = "line1\nLINE2\nline3\nline4"
    diff = _make_diff(new, "@@ -1,3 +1,4 @@", [" line1", "-line2", "+LINE2", " line3", "+line4"])
    assert apply_unified_diff(prev, diff) == new


def test_multi_hunk():
    prev = "1\n2\n3\n4\n5\n6"
    new = "1\nX\n3\n4\nY\n6"
    diff = _make_diff(
        new,
        "@@ -1,2 +1,2 @@",
        [" 1", "-2", "+X", "@@ -4,3 +4,3 @@", " 4", "-5", "+Y", " 6"],
    )
    assert apply_unified_diff(prev, diff) == new


def test_crlf_is_preserved():
    # Prior value uses CRLF; the diff body is LF-separated but content joins back with CRLF
    # (the sha256 in the +++ header is over the CRLF form).
    prev = "a\r\nb\r\nc"
    new = "a\r\nB\r\nc"
    diff = _make_diff(new, "@@ -1,3 +1,3 @@", [" a", "-b", "+B", " c"])
    assert apply_unified_diff(prev, diff) == new


def test_bad_hash_keeps_prev():
    prev = "line1\nline2\nline3"
    new = "line1\nLINE2\nline3"
    diff = _make_diff(new, "@@ -1,3 +1,3 @@", [" line1", "-line2", "+LINE2", " line3"], bad_hash=True)
    # Reconstructed value's hash won't match the (tampered) header -> keep the prior value.
    assert apply_unified_diff(prev, diff) == prev


def test_none_diff_keeps_prev():
    prev = "anything"
    assert apply_unified_diff(prev, None) == prev


def test_malformed_hunk_keeps_prev():
    prev = "line1\nline2"
    assert apply_unified_diff(prev, "--- old\n+++ x\n@@ not a real header @@\n+z") == prev


def test_unrecognized_body_line_keeps_prev():
    prev = "line1\nline2"
    new = "line1\nline2"
    # A body line that isn't ' '/'-'/'+' (here '!') makes _rebuild_from_hunks bail -> keep prev.
    diff = _make_diff(new, "@@ -1,2 +1,2 @@", [" line1", "!bogus", " line2"])
    assert apply_unified_diff(prev, diff) == prev


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
