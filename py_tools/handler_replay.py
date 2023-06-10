from py_tools.handler import aws_lambda_handler as main_aws_lambda_handler
from py_tools.dydb import DbModel

from py_tools.date import date_id
from py_tools import format
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, JSONAttribute
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration


class SimpleJSONAttribute(JSONAttribute):
    def serialize(self, value):
        """
        Serializes JSON to unicode
        """
        if value is None:
            return None
        encoded = format.dumps(value)
        return encoded


class ReplayBin(DbModel):
    nickname = "replay"

    class Meta:
        table_name = "ReplayBin"

    bin = UnicodeAttribute(hash_key=True)
    replay_id = UnicodeAttribute(
        default_for_new=date_id(nickname), range_key=True
    )
    run_count = NumberAttribute(default=1)
    record = SimpleJSONAttribute()
    reason = UnicodeAttribute()


def aws_lambda_handler(
    file,
    name=None,
    record_wrapper=None,
    before_request=None,
    sentry_dsn=None,
):
    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            integrations=[AwsLambdaIntegration(timeout_warning=True)],
            traces_sample_rate=1.0,
        )

    def wrapper(event, context=None):
        outpost = main_aws_lambda_handler(
            file, name, record_wrapper, before_request)(event, context)

        # add to replay table
        if outpost.replays:
            with ReplayBin.batch_write() as batch:
                for item in outpost.replays:
                    batch.save(ReplayBin.save_attributes(item))

        if not outpost.processed:
            return

        return outpost.processed if outpost.many else outpost.processed[0]

    return wrapper


def aws_lambda_replay_handler(file, name=None, record_wrapper=None, before_request=None, sentry_dsn=None):
    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            integrations=[AwsLambdaIntegration(timeout_warning=True)],
            traces_sample_rate=1.0,
        )
    file = file.replace("/adhoc/", "/")

    def wrapper(event=None, context=None):

        for item in ReplayBin.query(hash_key=name, limit=10):
            item = item.dict()

            outpost = main_aws_lambda_handler(
                file, name, record_wrapper, before_request)(item["record"], context)

            if outpost.replays:
                ReplayBin.update_item(
                    hash_key=name,
                    range_key=item["replay_id"],
                    adds={"run_count": 1}
                )
                return  # return early for events to process in order
            else:
                ReplayBin.delete_item(
                    hash_key=name,
                    range_key=item["replay_id"]
                )
    return wrapper
