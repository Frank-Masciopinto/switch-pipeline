class FatalPipelineError(Exception):
    """An error that retrying cannot fix (bug, misconfiguration, broken source contract).

    Workers let it terminate the process; everything else is retried with backoff.
    State is only ever advanced after a durable write, so a crash never loses data.
    """
