from collections.abc import Iterator

import pytest

from tests.helpers import settings_variables


@pytest.fixture(scope="session", autouse=True)
def hermetic_settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Neither the developer's .env nor exported settings variables reach a test."""
    with pytest.MonkeyPatch.context() as patch:
        for name in settings_variables():
            patch.delenv(name, raising=False)
        patch.chdir(tmp_path_factory.mktemp("workdir"))
        yield
