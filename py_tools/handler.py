import importlib.util
import os
from py_tools.dydb_utils import StreamRecord
import traceback
from py_tools.format import loads
import sentry_sdk


class Handlers:
    def __init__(self, file, record, context, record_wrapper=None, before_request=None):
        self.file = file
        self.record = record
        self.context = context
        self.record_wrapper = record_wrapper
        if before_request:
            before_request(record, context)

    def dynamodb(self):
        wrapper = self.record_wrapper or StreamRecord
        record = wrapper(self.record)
        m = self.module_handler(
            self.file, record.trigger_module, folder="dynamodb")
        functions = getattr(m, record.event_name, [])
        for function in functions:
            function(record, self.context)
        return

    def sqs(self):
        module_name = self.record["eventSourceARN"].split(
            ":")[-1].replace(".fifo", "")
        m = self.module_handler(self.file, module_name, folder="sqs")
        return m.handler(loads(self.record["body"]), self.record)

    def adhoc(self):
        m = self.module_handler(self.file, self.record["type"], folder="adhoc")
        return m.handler(self.record)

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


class OutPost:
    def __init__(self, many=True):
        self.replays = []
        self.processed = []
        self.many = many

    def __call__(self, many):
        self.many = many
        return self

    def add_processed(self, output):
        self.processed.append(output)

    def add_replays(self, output):
        self.replays.append(output)

    def process_failed(self, name, record, reason):
        entry = {"bin": name, "record": record, "reason": reason}
        self.add_replays(entry)


def aws_lambda_handler(file, name=None, record_wrapper=None, before_request=None, send_sentry=False):
    def handler(event, context):
        outpost = OutPost()

        if "Records" not in event:
            outpost(many=False)
            event.setdefault("eventSource", "aws:adhoc")
            event = {"Records": [event]}

        for record in event["Records"]:

            source_handler = record["eventSource"].split(":")[-1].lower()

            try:

                handler_cls = Handlers(
                    file, record, context, record_wrapper, before_request
                )

                method = getattr(handler_cls, source_handler)
                # run handler function
                output = method()
                # add to process list
                outpost.add_processed(output)

            except BaseException as e:
                # send to sentry
                if send_sentry:
                    sentry_sdk.set_context("record", record)
                    sentry_sdk.capture_exception(e)

                if source_handler != "adhoc":
                    outpost.process_failed(
                        name, record, traceback.format_exc())

        return outpost

    return handler
