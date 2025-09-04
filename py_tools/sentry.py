import os
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
import logging


def release_info():
    release = ""
    if os.environ.get("LAMBDA_TASK_ROOT"):
        release = open(f"{os.environ['LAMBDA_TASK_ROOT']}/release.txt").read()
    return release


def setup_sentry(sentry_dsn, integrations, release=None):
    log_level = logging.DEBUG
    send_default_pii = True
    if os.environ["ENVIRONMENT"] == "prd":
        log_level = logging.INFO
        send_default_pii = False
    sentry_sdk.init(
        dsn=sentry_dsn,
        integrations=[
            LoggingIntegration(level=log_level),
        ]
        + integrations,
        traces_sample_rate=1.0,
        attach_stacktrace=True,
        release=release or release_info(),
        send_default_pii=send_default_pii,
        environment=os.environ["ENVIRONMENT"],
        _experiments={
            "continuous_profiling_auto_start": True,
        },
        enable_logs=True,
    )
