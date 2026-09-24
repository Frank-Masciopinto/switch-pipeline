import logging
import signal
from collections.abc import Iterator

import pytest
import structlog

from tests.helpers import settings_variables


@pytest.fixture(scope="session", autouse=True)
def hermetic_settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Neither the developer's .env nor exported settings variables reach a test."""
    with pytest.MonkeyPatch.context() as patch:
        for name in settings_variables():
            patch.delenv(name, raising=False)
        patch.chdir(tmp_path_factory.mktemp("workdir"))
        yield


@pytest.fixture
def entrypoint_state() -> Iterator[None]:
    """Undo what an entry point sets up for the whole process: logging and signal handlers."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    signal_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    yield
    for sig, handler in signal_handlers.items():
        signal.signal(sig, handler)
    root.handlers[:] = handlers
    root.setLevel(level)
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
