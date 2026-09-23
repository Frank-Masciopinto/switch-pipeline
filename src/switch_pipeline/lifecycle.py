"""Process lifecycle: cooperative shutdown and liveness heartbeats."""

import signal
import threading
from pathlib import Path
from types import FrameType

import structlog

log = structlog.stdlib.get_logger(__name__)

_HEARTBEAT_SLICE_SECONDS = 5.0


class Shutdown:
    """Stop flag set by SIGTERM/SIGINT; waits on it wake up immediately."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def install_signal_handlers(self) -> "Shutdown":
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)
        return self

    def _on_signal(self, signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown_requested", signal=signal.Signals(signum).name)
        self._event.set()

    def request(self) -> None:
        self._event.set()

    def requested(self) -> bool:
        # A method, not a property: the flag flips asynchronously (signal
        # handler), so repeated checks must not be treated as one stable value.
        return self._event.is_set()

    def sleep(self, seconds: float) -> bool:
        """Wait up to ``seconds``; return True if shutdown was requested."""
        return self._event.wait(max(seconds, 0.0))


class Heartbeat:
    """Touches a file so the container healthcheck can tell the loop is alive."""

    def __init__(self, path: Path | None) -> None:
        self._path = path

    def beat(self) -> None:
        if self._path is not None:
            self._path.touch()


def idle(seconds: float, *, shutdown: Shutdown, heartbeat: Heartbeat) -> bool:
    """Sleep in slices while keeping the heartbeat fresh; True if shutdown was requested."""
    remaining = seconds
    while remaining > 0:
        heartbeat.beat()
        step = min(remaining, _HEARTBEAT_SLICE_SECONDS)
        if shutdown.sleep(step):
            return True
        remaining -= step
    heartbeat.beat()
    return shutdown.requested()
