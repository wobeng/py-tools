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
from py_tools.pylog import get_logger


logger = get_logger("py-tools.handler")


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
        logger.debug("Loading module %s.%s from %s", folder, module_name, path)
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
        logger.debug("Initialized handler for source %s", record.get("eventSource"))

    def dynamodb(self, target_function_name=None):
        wrapper = self.dydb_wrapper or StreamRecord
        record = wrapper(self.record)
        m = self.module_handler(self.file, record.trigger_module, folder="dynamodb")
        functions = getattr(m, record.event_name, [])

        # If target_function_name specified, only run that function
        if target_function_name:
            for function in functions:
                if function.__name__ == target_function_name:
                    function(record, self.context)
                    break
        else:
            for function in functions:
                function(record, self.context)
        return

    def sqs(self):
        module_name = self.record["eventSourceARN"].split(":")[-1].replace(".fifo", "")
        m = self.module_handler(self.file, module_name, folder="sqs")
        if not hasattr(m, "handler"):
            return
        logger.info("Processing SQS message for %s", module_name)
        return m.handler(loads(self.record["body"]), self.record)

    def adhoc(self):
        if "type" not in self.record:
            return {"type": "type is missing. Please a file name in adhoc folder."}
        m = self.module_handler(self.file, self.record["type"], folder="adhoc")
        if not hasattr(m, "handler"):
            return
        logger.info("Processing adhoc event %s", self.record["type"])
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
            logger.debug(
                "No batch_handler on %s, falling back to individual", module_name
            )
            return False

        bodies = [loads(r["body"]) for r in self.records]
        return m.batch_handler(bodies, self.records)

    def dynamodb(self):
        """Process multiple DynamoDB stream records as a batch.
        Looks for batch_{event_name} in the dynamodb module. If not found,
        return False so individual processing can handle each record.
        """
        if not self.records:
            return []

        stream_records = [StreamRecord(r) for r in self.records]
        module_name = stream_records[0].trigger_module
        m = self.module_handler(self.file, module_name, folder="dynamodb")

        batch_func_name = f"batch_{stream_records[0].event_name}"
        if not hasattr(m, batch_func_name):
            logger.debug(
                "No batch function %s on %s, falling back to individual",
                batch_func_name,
                module_name,
            )
            return False

        batch_func = getattr(m, batch_func_name)
        return batch_func(stream_records, self.context)


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

    def process_failed(self, name, record, reason, function_name=None):
        entry = {"bin": name, "reason": reason}

        # Store function name if provided (for DynamoDB granular replay)
        if function_name:
            entry["function_name"] = function_name

        # If S3 bucket specified, store record there; else compress in DynamoDB
        if self.s3_bucket:
            entry["s3_uri"] = store_record_s3(
                record, self.s3_bucket, self.s3_key_prefix
            )
        else:
            entry["record"] = compress_json(record)

        logger.warning(
            "Queued replay for %s (function: %s)",
            name,
            function_name or "n/a",
        )
        self.add_replays(entry)


def _process_batch(file, records, context, source_handler, name, send_sentry, outpost):
    """Process records as a batch"""
    batch_handler_cls = BatchHandlers(file, records, context)

    if not hasattr(batch_handler_cls, source_handler):
        return False

    try:
        method = getattr(batch_handler_cls, source_handler)
        output = method()
        if output is False:
            return False
        if output:
            outpost.add_processed(output)
            logger.info(
                "Batch %s processed %d outputs", source_handler, len(outpost.processed)
            )
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
        logger.warning("Batch %s failed; stored for replay", source_handler)
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
    target_function_name=None,
):
    """Process a single record"""
    # Special handling for DynamoDB to track individual function failures
    if source_handler == "dynamodb" and not target_function_name:
        handler_cls = Handlers(file, record, context, dydb_wrapper, before_request)
        wrapper = dydb_wrapper or StreamRecord
        stream_record = wrapper(record)
        m = handler_cls.module_handler(
            file, stream_record.trigger_module, folder="dynamodb"
        )
        functions = getattr(m, stream_record.event_name, [])

        for function in functions:
            try:
                function(stream_record, context)
            except BaseException:
                if send_sentry:
                    sentry_sdk.set_context("record", record)
                    sentry_sdk.set_context("function", function.__name__)
                    sentry_sdk.capture_exception()

                # Store individual function failure
                outpost.process_failed(
                    name,
                    record,
                    traceback.format_exc(),
                    function_name=function.__name__,
                )
        return

    # Standard processing for other sources or targeted replay
    try:
        handler_cls = Handlers(file, record, context, dydb_wrapper, before_request)
        method = getattr(handler_cls, source_handler)
        if source_handler == "dynamodb" and target_function_name:
            output = method(target_function_name=target_function_name)
        else:
            output = method()
        if output:
            outpost.add_processed(output)
            logger.debug("Processed individual %s event", source_handler)
    except BaseException:
        if send_sentry:
            sentry_sdk.set_context("record", record)
            if target_function_name:
                sentry_sdk.set_context("function", target_function_name)
            sentry_sdk.capture_exception()

        if source_handler == "adhoc":
            raise

        outpost.process_failed(
            name, record, traceback.format_exc(), function_name=target_function_name
        )
        logger.warning("%s record failed; queued for replay", source_handler)


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

        logger.info("Handling %d record(s) for %s", len(records), source_handler)

        # batch processing
        processed_in_batch = _process_batch(
            file, records, context, source_handler, name, send_sentry, outpost
        )

        # Skip individual processing when a batch handler successfully handled the set
        if not processed_in_batch:
            for record in records:
                # Extract target function name if present (for targeted replay)
                target_function_name = record.pop("_target_function_name", None)

                if target_function_name:
                    logger.debug(
                        "Targeted replay for function %s", target_function_name
                    )

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
                    target_function_name=target_function_name,
                )

        return outpost

    return handler
