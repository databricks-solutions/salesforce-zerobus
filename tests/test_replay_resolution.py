"""Tests for replay-position resolution in salesforce_zerobus/core.py.

The old thread + `threading.Event` startup choreography (which raced table creation and
could silently fall back to LATEST, skipping the historical backfill) has been replaced by
a single synchronous `_resolve_replay_params()` that the async pipeline awaits via
`asyncio.to_thread` before subscribing. These tests drive that method directly with a fake
replay manager.

Run with the project venv (core.py imports avro/grpc/zerobus):
    .venv/bin/python -m pytest tests/test_replay_resolution.py
"""

import logging

from salesforce_zerobus.core import SalesforceZerobus

logging.disable(logging.CRITICAL)


class _FakeReplayManager:
    """Records how it was called and returns a canned replay decision."""

    def __init__(self, result=("EARLIEST", ""), raises=False):
        self.result = result
        self.raises = raises
        self.recovery_initialized = False
        self.last_call = None

    def get_subscription_params(self, auto_create_table, backfill_historical):
        self.last_call = (auto_create_table, backfill_historical)
        if self.raises:
            raise RuntimeError("simulated init failure")
        return self.result

    def initialize_replay_recovery(self):
        self.recovery_initialized = True


def _bare_streamer(replay_manager):
    s = object.__new__(SalesforceZerobus)  # skip __init__ (needs real auth/config)
    s.logger = logging.getLogger("test")
    s._replay_manager = replay_manager
    s.auto_create_table = True
    s.backfill_historical = True
    return s


def test_resolves_earliest_and_creates_table():
    """Table creation + backfill decision happen in one call; EARLIEST is honored."""
    rm = _FakeReplayManager(result=("EARLIEST", ""))
    s = _bare_streamer(rm)
    assert s._resolve_replay_params() == ("EARLIEST", "")
    # Resolution owns table creation (auto_create_table=True) and pre-fetches the replay id.
    assert rm.last_call == (True, True)
    assert rm.recovery_initialized is True


def test_custom_resume_is_preserved():
    """A stored replay id resumes (CUSTOM), rather than re-running the backfill."""
    s = _bare_streamer(_FakeReplayManager(result=("CUSTOM", "REPLAYID123")))
    assert s._resolve_replay_params() == ("CUSTOM", "REPLAYID123")


def test_no_replay_manager_uses_latest():
    """With replay recovery disabled there is no manager; start fresh from LATEST."""
    s = _bare_streamer(None)
    assert s._resolve_replay_params() == ("LATEST", "")


def test_failure_falls_back_to_latest():
    """A replay-manager error must not crash startup; fall back to LATEST."""
    s = _bare_streamer(_FakeReplayManager(raises=True))
    assert s._resolve_replay_params() == ("LATEST", "")


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
