from py_tools.handler import aws_lambda_handler as main_aws_lambda_handler
from py_tools.dydb import DbModel
from py_tools.date import date_id
from pynamodb.attributes import UnicodeAttribute, NumberAttribute
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
import gzip
import base64
from py_tools.format import loads
from py_tools.sentry import setup_sentry
from sentry_sdk.scrubber import EventScrubber, DEFAULT_DENYLIST, DEFAULT_PII_DENYLIST


def decompress_json(compressed_string):
    compressed_data = base64.b64decode(compressed_string.encode())
    decompressed_data = gzip.decompress(compressed_data)
    decompressed_json_string = decompressed_data.decode()
    data = loads(decompressed_json_string)
    return data


class ReplayBin(DbModel):
    nickname = "replay"

    class Meta:
        table_name = "ReplayBin"

    bin = UnicodeAttribute(hash_key=True)
    replay_id = UnicodeAttribute(default_for_new=date_id(nickname), range_key=True)
    run_count = NumberAttribute(default=1)
    record = UnicodeAttribute()
    reason = UnicodeAttribute()


def aws_lambda_handler(
    file,
    name=None,
    record_wrapper=None,
    before_request=None,
    sentry_dsn=None,
    sentry_denylist=None,
    sentry_pii_denylist=None,
):
    if sentry_dsn:
        sentry_denylist = (sentry_denylist or []) + DEFAULT_DENYLIST
        sentry_pii_denylist = (sentry_pii_denylist or []) + DEFAULT_PII_DENYLIST
        setup_sentry(
            sentry_dsn,
            [
                AwsLambdaIntegration(timeout_warning=True),
            ],
            send_default_pii=False,
            event_scrubber=EventScrubber(
                denylist=sentry_denylist, pii_denylist=sentry_pii_denylist
            ),
        )

    def wrapper(event, context=None):
        function = main_aws_lambda_handler(
            file=file,
            name=name,
            record_wrapper=record_wrapper,
            before_request=before_request,
            send_sentry=(sentry_dsn is not None),
        )

        outpost = function(event, context)
        # add to replay table
        if outpost.replays:
            with ReplayBin.batch_write() as batch:
                for item in outpost.replays:
                    batch.save(ReplayBin.save_attributes(item))

        if not outpost.processed:
            return

        return outpost.processed if outpost.many else outpost.processed[0]

    return wrapper


def aws_lambda_replay_handler(
    file,
    name=None,
    record_wrapper=None,
    before_request=None,
    sentry_dsn=None,
    sentry_denylist=None,
    sentry_pii_denylist=None,
):
    if sentry_dsn:
        sentry_denylist = (sentry_denylist or []) + DEFAULT_DENYLIST
        sentry_pii_denylist = (sentry_pii_denylist or []) + DEFAULT_PII_DENYLIST
        setup_sentry(
            sentry_dsn,
            [
                AwsLambdaIntegration(timeout_warning=True),
            ],
            send_default_pii=False,
            event_scrubber=EventScrubber(
                denylist=sentry_denylist, pii_denylist=sentry_pii_denylist
            ),
        )

    file = file.replace("/adhoc/", "/")

    def wrapper(event=None, context=None):
        function = main_aws_lambda_handler(
            file=file,
            name=name,
            record_wrapper=record_wrapper,
            before_request=before_request,
            send_sentry=(sentry_dsn is not None),
        )

        for item in ReplayBin.query(hash_key=name, limit=10):
            item = item.dict()
            record = decompress_json(item["record"])

            outpost = function(record, context)

            if outpost.replays:
                ReplayBin.update_item(
                    hash_key=name, range_key=item["replay_id"], adds={"run_count": 1}
                )
                return  # return early for events to process in order
            else:
                ReplayBin.delete_item(hash_key=name, range_key=item["replay_id"])

    return wrapper
