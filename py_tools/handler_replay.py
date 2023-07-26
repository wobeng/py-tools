from py_tools.handler import aws_lambda_handler as main_aws_lambda_handler
from py_tools.dydb import DbModel
import os
from py_tools.date import date_id
from py_tools import format
from pynamodb.attributes import UnicodeAttribute, NumberAttribute
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
import gzip



class ReplayBin(DbModel):
    nickname = "replay"

    class Meta:
        table_name = "ReplayBin"

    bin = UnicodeAttribute(hash_key=True)
    replay_id = UnicodeAttribute(
        default_for_new=date_id(nickname), range_key=True
    )
    run_count = NumberAttribute(default=1)
    record = UnicodeAttribute()
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
             environment=os.environ["ENVIRONMENT"]
        )

    def wrapper(event, context=None):

        function = main_aws_lambda_handler(
            file = file, 
            name = name, 
            record_wrapper=record_wrapper, 
            before_request=before_request, 
            send_sentry=(sentry_dsn is not None)
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


def aws_lambda_replay_handler(file, 
                              name=None, 
                              record_wrapper=None, 
                              before_request=None, 
                              sentry_dsn=None
    ):
    if sentry_dsn:
        sentry_sdk.init(
            dsn=sentry_dsn,
            integrations=[AwsLambdaIntegration(timeout_warning=True)],
            environment=os.environ["ENVIRONMENT"]
        )
    file = file.replace("/adhoc/", "/")

    def wrapper(event=None, context=None):
        function = main_aws_lambda_handler(
            file = file, 
            name = name, 
            record_wrapper=record_wrapper, 
            before_request=before_request, 
            send_sentry=(sentry_dsn is not None)
            )
            
        for item in ReplayBin.query(hash_key=name, limit=10):
            item = item.dict()
            decompressed_data = gzip.decompress(item["record"])
            record = format.loads(decompressed_data.decode('utf-8'))

            outpost = function(record, context)

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
