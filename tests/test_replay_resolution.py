"""Tests for the SalesforceZerobus startup-race fix in salesforce_zerobus/core.py.

Background: `start()` used to `time.sleep(2)` and then resolve the subscription's replay
mode with `auto_create_table=False`. On a slow/cold warehouse the background table-init
hadn't finished, so the table didn't exist yet and the subscription fell back to LATEST,
silently skipping the historical backfill. The fix waits on `_init_complete` and has the
subscription reuse the replay decision the init path already computed
(`_init_replay_params`).

These tests build a bare SalesforceZerobus (bypassing the heavy __init__) and drive the
two methods directly with a fake replay manager.

Run with the project venv (core.py imports avro/grpc/zerobus):
    .venv/bin/python tests/test_replay_init_race.py
Or:  .venv/bin/python -m pytest tests/test_replay_init_race.py
"""

import asyncio
import logging
import threading
import time

from salesforce_zerobus.core import SalesforceZerobus

logging.disable(logging.CRITICAL)


class _FakeReplayManager:
    """Mimics the race: creating the table (auto_create_table=True) is slow and yields
    EARLIEST; the no-create path (the old subscription call) returns LATEST."""

    def __init__(self, create_delay=0.5, raises=False):
        self.create_delay = create_delay
        self.raises = raises

    def get_subscription_params(self, auto_create_table, backfill_historical):
        if self.raises:
            raise RuntimeError("simulated init failure")
        if auto_create_table:
            time.sleep(self.create_delay)  # slow CREATE TABLE on a cold warehouse
            return ("EARLIEST", "")
        return ("LATEST", "")  # table not there yet -> fallback

    def initialize_replay_recovery(self):
        pass


def _bare_streamer(replay_manager):
    s = object.__new__(SalesforceZerobus)  # skip __init__ (needs real auth/config)
    s.logger = logging.getLogger("test")
    s._replay_manager = replay_manager
    s._databricks_forwarder = None
    s.auto_create_table = True
    s.backfill_historical = True
    s._init_complete = threading.Event()
    s._init_replay_params = None
    return s


def test_subscription_waits_and_uses_earliest():
    """The fix: wait for the slow init, then reuse its EARLIEST decision (not LATEST)."""
    s = _bare_streamer(_FakeReplayManager(create_delay=0.5))
    threading.Thread(target=lambda: asyncio.run(s._initialize_databricks_async()), daemon=True).start()

    assert s._init_complete.wait(timeout=10), "init never signalled completion"
    assert s._init_replay_params == ("EARLIEST", "")
    assert s._get_subscription_params() == ("EARLIEST", "")


def test_no_wait_would_fall_back_to_latest():
    """Guard documenting the original bug: resolving before init cached a result hits the
    auto_create_table=False path, which returns LATEST."""
    s = _bare_streamer(_FakeReplayManager())
    # init has NOT run, so _init_replay_params is still None
    assert s._init_replay_params is None
    assert s._get_subscription_params() == ("LATEST", "")


def test_custom_resume_is_preserved():
    """When init resolves to a stored replay_id, restarts resume (CUSTOM), not re-backfill."""
    s = _bare_streamer(_FakeReplayManager())
    s._init_replay_params = ("CUSTOM", "REPLAYID123")
    assert s._get_subscription_params() == ("CUSTOM", "REPLAYID123")


def test_init_signals_completion_even_on_error():
    """_init_complete must be set in a finally so start() never blocks forever."""
    s = _bare_streamer(_FakeReplayManager(raises=True))
    try:
        asyncio.run(s._initialize_databricks_async())
    except RuntimeError:
        pass
    assert s._init_complete.is_set()


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
