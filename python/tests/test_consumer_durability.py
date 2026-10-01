"""Tests for the batched, at-least-once consumer in salesforce_zerobus/core.py.

Drives `_consume` / `_flush_batch` with a fake forwarder + stream (no network) to assert:
  - records are ingested in batches (not one-by-one),
  - the durable checkpoint advances only AFTER wait_for_offset (durability barrier),
  - a ZerobusException recreates the stream and retries the SAME batch (no drop),
  - shutdown drains the queue and flushes the final partial batch.

Run:  .venv/bin/python -m pytest tests/test_consumer_durability.py
"""

import asyncio
import logging

from zerobus.sdk.shared import ZerobusException

from salesforce_zerobus.core import _SHUTDOWN_SENTINEL, SalesforceZerobus

logging.disable(logging.CRITICAL)


class _FakeForwarder:
    """Minimal stand-in for DatabricksForwarder used by the consumer."""

    def __init__(self, fail_times=0):
        self.fail_times = fail_times  # how many times ingest_batch raises before succeeding
        self.ingest_batch_sizes = []  # length of each successful batch ingested
        self.ingested_replays = []  # flattened replay ids that were durably ingested
        self.nowait_batch_sizes = []  # length of each fire-and-forget batch
        self.waited_offsets = []
        self.recreate_count = 0
        self.order = []  # ordered log of ('ingest'|'wait') for the barrier assertion
        self._offset = 0

    def build_record(self, decoded, org_id, payload_binary, schema_json):
        return {"replay_id": decoded["replay_id"]}

    async def ingest_batch(self, records):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise ZerobusException("simulated permanent stream failure")
        self.ingest_batch_sizes.append(len(records))
        self.ingested_replays.extend(r["replay_id"] for r in records)
        self._offset += len(records)
        self.order.append(("ingest", self._offset))
        return self._offset

    async def ingest_batch_nowait(self, records):
        self.nowait_batch_sizes.append(len(records))
        self.ingested_replays.extend(r["replay_id"] for r in records)
        self.order.append(("nowait", len(records)))

    async def wait_durable(self, offset):
        self.order.append(("wait", offset))
        self.waited_offsets.append(offset)

    async def recreate(self):
        self.recreate_count += 1


def _bare(forwarder, ingest_batch_size=100, flush_interval=0.05, wait_for_durability=True):
    s = object.__new__(SalesforceZerobus)
    s.logger = logging.getLogger("test")
    s._databricks_forwarder = forwarder
    s.org_id = "00Dxxx"
    s.ingest_batch_size = ingest_batch_size
    s.flush_interval_seconds = flush_interval
    s.max_batch_retries = 5
    s.wait_for_durability = wait_for_durability
    s._last_durable_replay_id = None
    return s


def _pkg(replay_id):
    return {
        "decoded_event": {"replay_id": replay_id},
        "payload_binary": b"",
        "schema_json": "",
    }


async def _run_consume(streamer, replay_ids):
    q = asyncio.Queue()
    for rid in replay_ids:
        await q.put(_pkg(rid))
    await q.put(_SHUTDOWN_SENTINEL)
    await streamer._consume(q)


def test_batches_and_advances_checkpoint_after_durability():
    fwd = _FakeForwarder()
    s = _bare(fwd, ingest_batch_size=100)
    asyncio.run(_run_consume(s, ["r0", "r1", "r2", "r3", "r4"]))

    # All 5 drained as a single batch on shutdown (batch size not reached mid-stream).
    assert fwd.ingest_batch_sizes == [5]
    assert fwd.ingested_replays == ["r0", "r1", "r2", "r3", "r4"]
    # Durability barrier: wait_for_offset called for the batch's offset.
    assert fwd.waited_offsets == [5]
    # Checkpoint advances only after ingest+wait, to the batch's last (max) replay id.
    assert fwd.order == [("ingest", 5), ("wait", 5)]
    assert s._last_durable_replay_id == "r4"


def test_flushes_when_batch_size_reached():
    fwd = _FakeForwarder()
    s = _bare(fwd, ingest_batch_size=2)
    asyncio.run(_run_consume(s, ["r0", "r1", "r2", "r3", "r4"]))

    # 2 + 2 mid-stream, then the trailing 1 on shutdown drain.
    assert fwd.ingest_batch_sizes == [2, 2, 1]
    assert fwd.ingested_replays == ["r0", "r1", "r2", "r3", "r4"]
    assert s._last_durable_replay_id == "r4"


def test_no_drop_on_zerobus_exception():
    # First ingest attempt fails permanently -> recreate + retry the SAME batch.
    fwd = _FakeForwarder(fail_times=1)
    s = _bare(fwd, ingest_batch_size=100)
    asyncio.run(_run_consume(s, ["r0", "r1", "r2"]))

    assert fwd.recreate_count == 1
    # The same batch is retried in full; nothing is dropped.
    assert fwd.ingest_batch_sizes == [3]
    assert fwd.ingested_replays == ["r0", "r1", "r2"]
    assert s._last_durable_replay_id == "r2"


def test_checkpoint_not_advanced_until_success():
    # Fail twice, succeed on the third try; checkpoint must only reflect the success.
    fwd = _FakeForwarder(fail_times=2)
    s = _bare(fwd, ingest_batch_size=100)
    asyncio.run(_run_consume(s, ["r0", "r1"]))

    assert fwd.recreate_count == 2
    assert fwd.ingest_batch_sizes == [2]
    assert s._last_durable_replay_id == "r1"


def test_fire_and_forget_uses_nowait_without_waiting():
    # wait_for_durability=False -> submit via nowait, never call the durability barrier.
    fwd = _FakeForwarder()
    s = _bare(fwd, ingest_batch_size=100, wait_for_durability=False)
    asyncio.run(_run_consume(s, ["r0", "r1", "r2"]))

    assert fwd.nowait_batch_sizes == [3]
    assert fwd.ingest_batch_sizes == []  # durable path not used
    assert fwd.waited_offsets == []  # no wait_for_offset
    assert fwd.order == [("nowait", 3)]
    assert fwd.ingested_replays == ["r0", "r1", "r2"]
    assert s._last_durable_replay_id == "r2"


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
