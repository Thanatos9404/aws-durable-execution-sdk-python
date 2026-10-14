"""Bound provider I/O without spawning replacement workers for a stuck call."""

import contextvars
import threading
import time

import pytest

from aws_durable_execution_sdk_python_otel import _flush


def _wait_idle() -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        with _flush._lock:
            if _flush._active is None:
                return
        time.sleep(0.001)
    raise AssertionError("Provider worker did not actually complete")


@pytest.fixture(autouse=True)
def idle_flush(monkeypatch):
    _wait_idle()
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 1.0)
    yield
    _wait_idle()


def test_timeout_retains_one_worker_and_rejects_new_work(monkeypatch):
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 0.02)
    started = threading.Event()
    release = threading.Event()
    calls = []

    def blocked():
        calls.append(threading.get_ident())
        started.set()
        assert release.wait(2)
        return True

    try:
        began = time.monotonic()
        assert _flush.flush_provider(blocked) is False
        assert time.monotonic() - began < 1
        assert started.wait(1)
        for _ in range(5):
            assert _flush.flush_provider(lambda: calls.append("unexpected")) is False
        assert len(calls) == 1
        workers = [t for t in threading.enumerate() if t.name == "durable-otel-flush"]
        assert len(workers) == 1
        assert workers[0].daemon
    finally:
        release.set()
        _wait_idle()
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 1.0)
    assert _flush.flush_provider(lambda: True) is True


@pytest.mark.parametrize("result", [True, False, None])
def test_completed_provider_result(result):
    assert _flush.flush_provider(lambda: result) is (result is not False)


@pytest.mark.parametrize(
    "error", [RuntimeError("ordinary"), SystemExit("fatal"), KeyboardInterrupt()]
)
def test_fast_error_preserves_identity_on_the_caller(error):
    owner = threading.get_ident()
    worker = []

    def fail():
        worker.append(threading.get_ident())
        raise error

    with pytest.raises(type(error)) as caught:
        _flush.flush_provider(fail)
    assert caught.value is error
    assert worker[0] != owner
    assert threading.get_ident() == owner


@pytest.mark.parametrize(
    "error", [RuntimeError("late ordinary"), SystemExit("late fatal")]
)
def test_late_error_is_reported_without_poisoning_next_call(error, monkeypatch, caplog):
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 0.02)
    release = threading.Event()

    def fail_late():
        assert release.wait(2)
        raise error

    try:
        assert _flush.flush_provider(fail_late) is False
    finally:
        release.set()
        _wait_idle()
    messages = [
        r for r in caplog.records if "after the caller stopped waiting" in r.message
    ]
    assert len(messages) == 1
    assert messages[0].exc_info[1] is error
    monkeypatch.setattr(_flush, "_FLUSH_TIMEOUT_SECONDS", 1.0)
    assert _flush.flush_provider(lambda: True) is True


def test_start_failure_releases_capacity(monkeypatch):
    def fail_start(_thread):
        raise RuntimeError("start failed")

    with monkeypatch.context() as patch:
        patch.setattr(threading.Thread, "start", fail_start)
        with pytest.raises(RuntimeError, match="start failed"):
            _flush.flush_provider(lambda: True)
    assert _flush.flush_provider(lambda: True) is True


def test_only_provider_io_gets_post_cleanup_context_snapshot():
    value = contextvars.ContextVar("flush-test", default="unset")
    token = value.set("owner-after-cleanup")
    try:

        def flush():
            assert value.get() == "owner-after-cleanup"
            value.set("provider-only")
            return True

        assert _flush.flush_provider(flush)
        assert value.get() == "owner-after-cleanup"
    finally:
        value.reset(token)
