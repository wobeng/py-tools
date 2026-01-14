from py_tools.handler import aws_lambda_handler as main_aws_lambda_handler
from py_tools.dydb import DbModel
from py_tools.date import date_id
from pynamodb.attributes import UnicodeAttribute, NumberAttribute
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
import gzip
import base64
from py_tools.format import loads
from py_tools.sentry import setup_sentry
from py_tools.s3 import get_object


def decompress_json(compressed_string):
    compressed_data = base64.b64decode(compressed_string.encode())
    decompressed_data = gzip.decompress(compressed_data)
    decompressed_json_string = decompressed_data.decode()
    data = loads(decompressed_json_string)
    return data


def fetch_record_from_s3(s3_uri):
    """Parse s3_uri and fetch record from S3."""
    parts = s3_uri.replace("s3://", "").split("/", 1)
    bucket, key = parts[0], parts[1]
    data = get_object(bucket, key)
    return loads(data)


class ReplayBin(DbModel):
    nickname = "replay"

    class Meta:
        table_name = "ReplayBin"

    bin = UnicodeAttribute(hash_key=True)
    replay_id = UnicodeAttribute(default_for_new=date_id(nickname), range_key=True)
    run_count = NumberAttribute(default=1)
    record = UnicodeAttribute(null=True)
    s3_uri = UnicodeAttribute(null=True)
    reason = UnicodeAttribute()
    function_name = UnicodeAttribute(null=True)  # For DynamoDB granular replay


def aws_lambda_handler(
    file,
    name,
    dydb_wrapper=None,
    before_request=None,
    sentry_dsn=None,
    sentry_denylist=None,
    sentry_pii_denylist=None,
    s3_bucket=None,
    s3_key_prefix="replay",
):
    if sentry_dsn:
        setup_sentry(
            sentry_dsn,
            [
                AwsLambdaIntegration(timeout_warning=True),
            ],
            sentry_denylist=sentry_denylist,
            sentry_pii_denylist=sentry_pii_denylist,
        )

    def wrapper(event, context=None):
        function = main_aws_lambda_handler(
            file=file,
            name=name,
            dydb_wrapper=dydb_wrapper,
            before_request=before_request,
            send_sentry=(sentry_dsn is not None),
            s3_bucket=s3_bucket,
            s3_key_prefix=s3_key_prefix,
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
    name,
    dydb_wrapper=None,
    before_request=None,
    sentry_dsn=None,
    sentry_denylist=None,
    sentry_pii_denylist=None,
    s3_bucket=None,
    s3_key_prefix="replay",
):
    if sentry_dsn:
        setup_sentry(
            sentry_dsn,
            [
                AwsLambdaIntegration(timeout_warning=True),
            ],
            sentry_denylist=sentry_denylist,
            sentry_pii_denylist=sentry_pii_denylist,
        )

    file = file.replace("/adhoc/", "/")

    def wrapper(event=None, context=None):
        function = main_aws_lambda_handler(
            file=file,
            name=name,
            dydb_wrapper=dydb_wrapper,
            before_request=before_request,
            send_sentry=(sentry_dsn is not None),
            s3_bucket=s3_bucket,
            s3_key_prefix=s3_key_prefix,
        )

        for item in ReplayBin.query(hash_key=name, limit=10):
            item = item.dict()

            # Fetch record from S3 or decompress from DynamoDB
            if item.get("s3_uri"):
                record = fetch_record_from_s3(item["s3_uri"])
            else:
                record = decompress_json(item["record"])

            # For DynamoDB with function_name, do targeted replay
            target_function = item.get("function_name")
            if target_function:
                # Inject target function into event for targeted replay
                record["_target_function_name"] = target_function
            
            outpost = function(record, context)

            if outpost.replays:
                ReplayBin.update_item(
                    hash_key=name, range_key=item["replay_id"], adds={"run_count": 1}
                )
                return  # return early for events to process in order
            else:
                ReplayBin.delete_item(hash_key=name, range_key=item["replay_id"])

    return wrapper
