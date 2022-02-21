import importlib.util
import os
from py_tools.dydb_utils import StreamRecord
import traceback
from py_tools.format import loads, dumps
from py_tools.sqs import Sqs
from uuid import uuid4
import json


class Handlers:
    def __init__(
        self, file, record, context, record_wrapper=None, before_request=None
    ):
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
            self.file, record.trigger_module, folder="dynamodb"
        )
        functions = getattr(m, record.event_name, [])
        for function in functions:
            function(record, self.context)
        return

    def sqs(self):
        module_name = (
            self.record["eventSourceARN"].split(":")[-1].replace(".fifo", "")
        )
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


class InPost:
    @staticmethod
    def generate_attributes(record):
        receive_count = 1
        source_handler = (
            record["eventSource"].split(":")[-1].lower()
        )  # should be dynamodb, sqs or adhoc
        return receive_count, source_handler

    @staticmethod
    def generate_replay_attributes(record):
        receive_count = int(
            record["messageAttributes"]["receive_count"]["stringValue"]
        )
        record = loads(record["body"])
        source_handler = (
            record.get("eventSource", "aws:adhoc").split(":")[-1].lower()
        )  # should be dynamodb or sqs
        return receive_count, source_handler, record

    @staticmethod
    def generate_dead_attributes(record):
        receive_count = int(
            record["messageAttributes"]["receive_count"]["stringValue"]
        )
        record = loads(record["body"])["record"]
        source_handler = (
            record.get("eventSource", "aws:adhoc").split(":")[-1].lower()
        )  # should be dynamodb or sqs
        return receive_count, source_handler, record


class OutPost:
    kills = []
    replays = []
    processed = []
    max_receive_count = 3

    @classmethod
    def add_processed(cls, output):
        cls.processed.append(output)

    @classmethod
    def add_replays(cls, output):
        cls.replays.append(output)

    @classmethod
    def add_kills(cls, output):
        cls.kills.append(output)

    @classmethod
    def process_failed(
        cls, name, record, source_handler, receive_count, reason
    ):
        uid = str(uuid4())
        entry = {
            "Id": uid,
            "MessageGroupId": name or source_handler,
            "MessageDeduplicationId": uid,
            "MessageAttributes": {
                "source": {
                    "StringValue": source_handler,
                    "DataType": "String",
                },
                "receive_count": {
                    "StringValue": str(receive_count + 1),
                    "DataType": "String",
                },
            },
        }
        if receive_count > cls.max_receive_count:
            entry["MessageBody"] = dumps({"record": record, "reason": reason})
            cls.add_kills(entry)
        else:
            entry["MessageBody"] = dumps(record)
            cls.add_replays(entry)
        print("Receive Count:====>\n\n{}".format(receive_count))

    @classmethod
    def ship(cls, queue_replay=None, queue_dead=None):
        # send back unprocessed later
        if cls.replays and queue_replay:
            Sqs(queue_replay).send_message_batch(cls.replays)
        # kill repetitive errors
        if cls.kills and queue_dead:
            Sqs(queue_dead).send_message_batch(cls.kills)


def process_queue_dead(handler, queue_dead):
    sqs = Sqs(queue_dead)
    messages = sqs.receive_messages(
        limit=1, WaitTimeSeconds=20, VisibilityTimeout=5
    )
    if "Messages" in messages:
        message = messages["Messages"][0]
        body = json.loads(message["Body"])["record"]
        output = handler({"Records": [body]}, None)
        sqs.delete_message(message["ReceiptHandle"])
        return output[0]


def aws_lambda_handler(
    file,
    name=None,
    record_wrapper=None,
    before_request=None,
    queue_replay=None,
    queue_dead=None,
):
    def handler(event, context):
        many = True

        if "Records" not in event:
            many = False
            event.setdefault("eventSource", "aws:adhoc")
            event = {"Records": [event]}

        for record in event["Records"]:

            receive_count, source_handler = InPost.generate_attributes(record)

            if source_handler == "sqs":
                queue_name = record["eventSourceARN"].split(":")[-1]

                # load orignal event attrs
                if queue_replay and queue_name == queue_replay:
                    (
                        receive_count,
                        source_handler,
                        record,
                    ) = InPost.generate_replay_attributes(record)

                if queue_dead and queue_name == queue_dead:
                    (
                        receive_count,
                        source_handler,
                        record,
                    ) = InPost.generate_dead_attributes(record)

            try:

                handler_cls = Handlers(
                    file, record, context, record_wrapper, before_request
                )
                method = getattr(handler_cls, source_handler)
                # run handler function
                output = method()
                # add to process list
                OutPost.add_processed(output)

            except BaseException:
                reason = traceback.format_exc()

                if source_handler != "adhoc":
                    OutPost.process_failed(
                        name, record, source_handler, receive_count, reason
                    )

                print(
                    "Unprocessed Record:====>\n\n{}".format(
                        dumps(record, indent=1)
                    )
                )
                print("Exception:====>\n\n{}".format(reason))

        # send back unprocessed later and kill repetitive errors
        OutPost.ship(queue_replay, queue_dead)

        if not OutPost.processed:
            return

        return OutPost.processed if many else OutPost.processed[0]

    return handler
