import importlib.util
import os
from py_tools.dydb_utils import StreamRecord
import traceback
import sentry_sdk
import gzip
import base64
from py_tools.format import dumps, loads
import tempfile
import uuid
from py_tools.s3 import upload_file


def compress_json(json_data):
    json_string = dumps(json_data, separators=(",", ":"))
    compressed_data = gzip.compress(json_string.encode())
    compressed_string = base64.b64encode(compressed_data).decode()
    return compressed_string


def store_record_s3(record, s3_bucket, s3_key_prefix="replay"):
    """Store record as uncompressed JSON in S3, return s3_uri."""
    uid = uuid.uuid4()
    key = f"{s3_key_prefix}/{uid}.json"
    with tempfile.NamedTemporaryFile(mode="w", delete=True) as f:
        f.write(dumps(record))
        f.flush()
        upload_file(f.name, s3_bucket, key)
    return f"s3://{s3_bucket}/{key}"


class BaseHandler:
    """Base handler with common functionality for single and batch processing"""

    @staticmethod
    def module_handler(file, module_name, folder="sqs"):
        path = os.path.dirname(os.path.realpath(file)) + "/{}/{}.py".format(
            folder, module_name
        )
        name = path.split("/")[-1].replace(".py", "")
        spec = importlib.util.spec_from_file_location(name, path)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m


class Handlers(BaseHandler):
    def __init__(self, file, record, context, dydb_wrapper=None, before_request=None):
        self.file = file
        self.record = record
        self.context = context
        self.dydb_wrapper = dydb_wrapper
        if before_request:
            before_request(record, context)

    def dynamodb(self):
        wrapper = self.dydb_wrapper or StreamRecord
        record = wrapper(self.record)
        m = self.module_handler(self.file, record.trigger_module, folder="dynamodb")
        functions = getattr(m, record.event_name, [])
        for function in functions:
            function(record, self.context)
        return

    def sqs(self):
        module_name = self.record["eventSourceARN"].split(":")[-1].replace(".fifo", "")
        m = self.module_handler(self.file, module_name, folder="sqs")
        if not hasattr(m, "handler"):
            return
        return m.handler(loads(self.record["body"]), self.record)

    def adhoc(self):
        m = self.module_handler(self.file, self.record["type"], folder="adhoc")
        if not hasattr(m, "handler"):
            return
        return m.handler(self.record, self.context)


class BatchHandlers(BaseHandler):
    def __init__(
        self,
        file,
        records,
        context,
    ):
        self.file = file
        self.records = records
        self.context = context

    def sqs(self):
        """Process multiple SQS records as a batch"""
        if not self.records:
            return []

        module_name = (
            self.records[0]["eventSourceARN"].split(":")[-1].replace(".fifo", "")
        )
        m = self.module_handler(self.file, module_name, folder="sqs")

        if not hasattr(m, "batch_handler"):
            return

        bodies = [loads(r["body"]) for r in self.records]
        return m.batch_handler(bodies, self.records)


class OutPost:
    def __init__(self, many=True, s3_bucket=None, s3_key_prefix="replay"):
        self.replays = []
        self.processed = []
        self.many = many
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key_prefix

    def __call__(self, many):
        self.many = many
        return self

    def add_processed(self, output):
        self.processed.append(output)

    def add_replays(self, output):
        self.replays.append(output)

    def process_failed(self, name, record, reason):
        entry = {"bin": name, "reason": reason}

        # If S3 bucket specified, store record there; else compress in DynamoDB
        if self.s3_bucket:
            entry["s3_uri"] = store_record_s3(
                record, self.s3_bucket, self.s3_key_prefix
            )
        else:
            entry["record"] = compress_json(record)

        self.add_replays(entry)


def _process_batch(file, records, context, source_handler, name, send_sentry, outpost):
    """Process records as a batch"""
    batch_handler_cls = BatchHandlers(file, records, context)

    if not hasattr(batch_handler_cls, source_handler):
        return False

    try:
        method = getattr(batch_handler_cls, source_handler)
        output = method()
        if output:
            outpost.add_processed(output)
        return True
    except BaseException:
        if send_sentry:
            sentry_sdk.set_context("records", dumps(batch_handler_cls.records))
            sentry_sdk.capture_exception()

        if source_handler == "adhoc":
            raise

        # Store entire batch as single replay record
        batch_event = {"Records": records}
        outpost.process_failed(name, batch_event, traceback.format_exc())
        return True


def _process_individual(
    file,
    record,
    context,
    dydb_wrapper,
    before_request,
    source_handler,
    name,
    send_sentry,
    outpost,
):
    """Process a single record"""
    try:
        handler_cls = Handlers(file, record, context, dydb_wrapper, before_request)
        method = getattr(handler_cls, source_handler)
        output = method()
        if output:
            outpost.add_processed(output)
    except BaseException:
        if send_sentry:
            sentry_sdk.set_context("record", record)
            sentry_sdk.capture_exception()

        if source_handler == "adhoc":
            raise

        outpost.process_failed(name, record, traceback.format_exc())


def aws_lambda_handler(
    file,
    name,
    dydb_wrapper=None,
    before_request=None,
    send_sentry=False,
    s3_bucket=None,
    s3_key_prefix="replay",
):
    def handler(event, context):
        outpost = OutPost(s3_bucket=s3_bucket, s3_key_prefix=s3_key_prefix)

        if "Records" not in event:
            outpost(many=False)
            event.setdefault("eventSource", "aws:adhoc")
            event = {"Records": [event]}

        records = event["Records"]
        source_handler = records[0]["eventSource"].split(":")[-1].lower()

        # batch processing
        processed_in_batch = _process_batch(
            file, records, context, source_handler, name, send_sentry, outpost
        )

        # Skip individual processing when a batch handler successfully handled the set
        if not processed_in_batch:
            for record in records:
                _process_individual(
                    file,
                    record,
                    context,
                    dydb_wrapper,
                    before_request,
                    source_handler,
                    name,
                    send_sentry,
                    outpost,
                )

        return outpost

    return handler
