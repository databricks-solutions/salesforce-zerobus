"""Fold Salesforce Change Data Capture events into a record's current state.

Pure Python (no Spark), so the semantics are unit-tested locally; the pipeline
(salesforce_cdc.py) calls `fold` once per record per micro-batch.

A record's state is a JSON object of its fields (nulls omitted; compound fields
such as BillingAddress are nested objects). Events apply in order:

- CREATE / UNDELETE carry the full record and replace the state.
- UPDATE is sparse: only fields named in changed_fields / nulled_fields /
  diff_fields change. For compound fields, Salesforce names the changed
  subfields (BillingAddress.City), and only those are applied, so untouched
  siblings survive. Fields in diff_fields carry a unified diff (large text
  fields) that is applied to the prior value.
- DELETE marks the record deleted and keeps its last fields; GAP_* events carry
  no field data (the record must be re-read from Salesforce) and change nothing.

Each record keeps a watermark: the (commit timestamp, commit number, sequence
number, replay ID) of the last event applied. Events at or below it are
skipped, so at-least-once redelivery can never roll a field back.
"""

import copy
import hashlib
import json
import re

HEADER = "ChangeEventHeader"
FULL_IMAGE = {"CREATE", "UNDELETE"}
GAP = {"GAP_CREATE", "GAP_UPDATE", "GAP_DELETE", "GAP_UNDELETE", "GAP_OVERFLOW"}


def seq_key(commit_ts, commit_number, sequence_number, replay_id):
    """Ordering key for an event; missing parts sort first."""
    return (
        commit_ts if commit_ts is not None else -1,
        commit_number if commit_number is not None else -1,
        sequence_number if sequence_number is not None else -1,
        replay_id or "",
    )


def fold(prior_fields, prior_deleted, prior_seq, events):
    """Apply events (dicts with change_type, changed_fields, nulled_fields,
    diff_fields, record, seq) to a record's prior state.

    prior_fields is the current state (dict) or None for a new record;
    prior_seq is its watermark (a seq_key tuple) or None.

    Returns None if every event was at or below the watermark, else a dict with
    fields, deleted, seq, and change_type of the last applied event.
    """
    fields = copy.deepcopy(prior_fields) if prior_fields else {}
    deleted = bool(prior_deleted)
    seq = tuple(prior_seq) if prior_seq is not None else None
    last = None
    for ev in sorted(events, key=lambda e: tuple(e["seq"])):
        ev_seq = tuple(ev["seq"])
        if seq is not None and ev_seq <= seq:
            continue  # redelivered, duplicate, or older than what is stored
        fields, deleted = apply_event(fields, deleted, ev)
        seq, last = ev_seq, ev
    if last is None:
        return None
    return {"fields": fields, "deleted": deleted, "seq": seq, "change_type": last["change_type"]}


def apply_event(fields, deleted, ev):
    """Apply one event to (fields, deleted); fields is modified in place."""
    change_type = ev["change_type"]
    record = ev.get("record") or {}
    if change_type in FULL_IMAGE:
        return _strip(record), False
    if change_type == "DELETE":
        return fields, True
    if change_type in GAP:
        return fields, deleted

    changed = list(ev.get("changed_fields") or [])
    nulled = set(ev.get("nulled_fields") or [])
    diffs = set(ev.get("diff_fields") or [])
    paths = list(dict.fromkeys(changed + sorted(nulled) + sorted(diffs)))
    # A compound field whose subfields are listed changes only those subfields.
    parents = {p[:i] for p in paths for i, c in enumerate(p) if c == "."}
    for path in paths:
        if path in parents or path == HEADER:
            continue
        if path in nulled:
            _delete(fields, path)
            continue
        value = _get(record, path)
        if path in diffs:
            value = _apply_unified_diff(_get(fields, path), value)
        if value is None:
            _delete(fields, path)
        else:
            _set(fields, path, copy.deepcopy(value))
    return fields, deleted


def loads(text):
    """Parse a JSON object (None for SQL NULL)."""
    return json.loads(text) if text else None


def dumps(fields):
    """Serialize state compactly for parse_json."""
    return json.dumps(fields, separators=(",", ":"), ensure_ascii=False)


def _strip(record):
    """The record's fields without the header and without nulls."""
    out = {}
    for k, v in record.items():
        if k == HEADER or v is None:
            continue
        if isinstance(v, dict):
            v = _strip(v)
            if not v:
                continue
        out[k] = copy.deepcopy(v)
    return out


def _get(obj, path):
    for part in path.split("."):
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


def _set(obj, path, value):
    parts = path.split(".")
    for part in parts[:-1]:
        nxt = obj.get(part)
        if not isinstance(nxt, dict):
            nxt = obj[part] = {}
        obj = nxt
    obj[parts[-1]] = value


def _delete(obj, path):
    """Remove path, then any parent objects it leaves empty."""
    parts = path.split(".")
    stack = []
    for part in parts[:-1]:
        nxt = obj.get(part) if isinstance(obj, dict) else None
        if not isinstance(nxt, dict):
            return
        stack.append((obj, part))
        obj = nxt
    obj.pop(parts[-1], None)
    for parent, key in reversed(stack):
        if parent[key]:
            break
        del parent[key]


# ---- Unified diffs (large text fields), from python/lakeflow_declarative_pipeline.py ----

_HUNK_RE = re.compile(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")


def _parse_unified_diff(diff_lines):
    """Split raw diff lines into (expected_hash, hunks).

    `expected_hash` is the SHA-256 carried in the `+++` header (or None). Each hunk is
    a (old_start, body_lines) pair, where old_start is the 1-based line in the previous
    value where the hunk begins and body_lines are its `' '`/`'-'`/`'+'` content lines.

    Returns None if a hunk header is malformed (the caller then keeps the prior value)."""
    expected_hash = None
    hunks = []
    pos, total = 0, len(diff_lines)
    while pos < total:
        line = diff_lines[pos]
        if line.startswith("+++ "):
            expected_hash = line[4:].strip() or None
            pos += 1
        elif line.startswith("@@"):
            match = _HUNK_RE.match(line)
            if not match:
                return None
            old_start = int(match.group(1))
            pos += 1
            body = []
            while pos < total:
                body_line = diff_lines[pos]
                if body_line[:2] == "@@" or body_line[:4] in ("--- ", "+++ "):
                    break  # start of the next hunk/header
                if body_line == "" and pos == total - 1:
                    pos += 1  # trailing artifact from split("\n")
                    continue
                body.append(body_line)
                pos += 1
            hunks.append((old_start, body))
        else:
            pos += 1  # "--- " header or any line outside a hunk
    return expected_hash, hunks


def _rebuild_from_hunks(prev_lines, hunks):
    """Apply parsed hunks to the previous value's lines, returning the new lines.

    Returns None on an unrecognized body line (the caller then keeps the prior value)."""
    result = []
    prev_idx = 0  # 0-based cursor into prev_lines
    for old_start, body in hunks:
        # Copy the unchanged lines that precede this hunk verbatim.
        while prev_idx < old_start - 1 and prev_idx < len(prev_lines):
            result.append(prev_lines[prev_idx])
            prev_idx += 1
        for body_line in body:
            tag = body_line[0] if body_line else " "
            if tag == " ":  # context: keep the prior line (fall back to diff text)
                result.append(prev_lines[prev_idx] if prev_idx < len(prev_lines) else body_line[1:])
                prev_idx += 1
            elif tag == "-":  # removal: drop the prior line
                prev_idx += 1
            elif tag == "+":  # addition: emit the new line
                result.append(body_line[1:])
            else:
                return None
    # Copy any unchanged lines after the last hunk.
    while prev_idx < len(prev_lines):
        result.append(prev_lines[prev_idx])
        prev_idx += 1
    return result


def _apply_unified_diff(prev, diff):
    """Reconstruct a field's full value by applying a Salesforce CDC unified diff to
    its previous value. Large text fields (>= 1000 chars) are sent as a unified diff
    (named in diff_fields) instead of the full value.

    Returns `prev` unchanged if the diff is missing/unparseable, or if the SHA-256 in
    the `+++` header doesn't match the reconstructed value (so we never store corrupt
    text — worst case we keep the last full value)."""
    if diff is None:
        return prev
    # The field value may use CRLF line endings while the diff body uses LF. Split the
    # prior value on its own terminator so line content matches the diff, and rejoin
    # with the same terminator (the SHA-256 in the +++ header is over the CRLF form).
    term = "\r\n" if (prev and "\r\n" in prev) else "\n"
    prev_lines = [] if prev is None else prev.split(term)
    try:
        parsed = _parse_unified_diff(diff.split("\n"))
        if parsed is None:
            return prev
        expected_hash, hunks = parsed
        result_lines = _rebuild_from_hunks(prev_lines, hunks)
        if result_lines is None:
            return prev
        result = term.join(result_lines)
    except Exception:
        return prev
    if expected_hash and hashlib.sha256(result.encode("utf-8")).hexdigest() != expected_hash:
        return prev
    return result

