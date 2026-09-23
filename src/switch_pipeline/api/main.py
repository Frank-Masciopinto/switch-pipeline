"""API entry point."""

import uvicorn

from switch_pipeline.api.app import create_app
from switch_pipeline.observability import configure_logging
from switch_pipeline.settings import (
    ApiSettings,
    KafkaSettings,
    LogSettings,
    PostgresSettings,
    load_settings,
)


def run_api() -> int:
    postgres = load_settings(PostgresSettings)
    api = load_settings(ApiSettings)
    kafka = load_settings(KafkaSettings)
    configure_logging(load_settings(LogSettings), service="api")
    app = create_app(postgres=postgres, api=api, kafka=kafka)
    # log_config=None keeps uvicorn on our JSON logging; requests are logged by
    # the middleware (with request ids), so the default access log is disabled.
    uvicorn.run(
        app,
        host=api.host,
        port=api.port,
        log_config=None,
        access_log=False,
        proxy_headers=True,
        server_header=False,
    )
    return 0
