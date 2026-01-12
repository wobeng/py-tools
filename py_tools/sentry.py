import os
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
import logging
from sentry_sdk.scrubber import EventScrubber, DEFAULT_DENYLIST, DEFAULT_PII_DENYLIST


def release_info():
    release = ""
    if os.environ.get("LAMBDA_TASK_ROOT"):
        release = open(f"{os.environ['LAMBDA_TASK_ROOT']}/release.txt").read()
    return release


def setup_sentry(
    sentry_dsn,
    integrations,
    release=None,
    sentry_denylist=None,
    sentry_pii_denylist=None,
):
    log_level = logging.DEBUG
    sentry_denylist = (sentry_denylist or []) + DEFAULT_DENYLIST
    sentry_pii_denylist = (sentry_pii_denylist or []) + DEFAULT_PII_DENYLIST
    if os.environ["ENVIRONMENT"] == "prd":
        log_level = logging.INFO
    sentry_sdk.init(
        dsn=sentry_dsn,
        integrations=[
            LoggingIntegration(level=log_level),
        ]
        + integrations,
        traces_sample_rate=1.0,
        attach_stacktrace=True,
        release=release or release_info(),
        send_default_pii=False,
        event_scrubber=EventScrubber(
            denylist=sentry_denylist, pii_denylist=sentry_pii_denylist
        ),
        environment=os.environ["ENVIRONMENT"],
        _experiments={
            "continuous_profiling_auto_start": True,
        },
        enable_logs=True,
    )
