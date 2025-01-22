from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
from py_tools import format
import tempfile
from py_tools.format import dumps
import uuid
from py_tools.s3 import upload_file


class Sqs:
    def __init__(self, queue_name=None, queue_url=None, bucket=None, key_prefix="sqs"):
        self.client = boto3.client("sqs")
        self.queue_url = queue_url
        self.bucket = bucket
        self.key_prefix = key_prefix
        if queue_name:
            self.queue_url = self.client.get_queue_url(QueueName=queue_name)["QueueUrl"]

    def _run_batch(self, entries, sqs_client_method):
        n = 10
        new_entries = [
            entries[i * n : (i + 1) * n] for i in range((len(entries) + n - 1) // n)
        ]

        for ne in new_entries:
            ne_copy = ne.copy()
            get_out = False
            while not get_out:
                response = getattr(self.client, sqs_client_method)(
                    QueueUrl=self.queue_url, Entries=ne_copy
                )
                if response.get("Failed", None):
                    failed_ids = [f["Id"] for f in response["Failed"]]
                    ne_copy = [e for e in ne if e["Id"] in failed_ids]
                else:
                    get_out = True

    def _store_sqs_s3(self, message):
        uid = uuid.uuid4()
        key = f"{self.key_prefix}/{uid}.json"
        with tempfile.NamedTemporaryFile(mode="w", delete=True) as f:
            f.write(dumps(message))
            f.flush()
            upload_file(
                f.name,
                self.bucket,
                key,
            )
        return {"bucket": self.bucket, "key": key}

    def purge(self):
        response = self.client.purge_queue(QueueUrl=self.queue_url)
        return response

    def delete_message(self, receipt_handle):
        response = self.client.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
        )
        return response

    def receive_messages(self, limit=10, **kwargs):
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=limit,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
            **kwargs,
        )
        return response

    def send_message(self, message, **kwargs):
        try:
            response = self.client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=format.dumps(message, use_decimal=True),
                **kwargs,
            )
        except ClientError as e:
            if "message must be shorter" in str(e).lower():
                response = self.client.send_message(
                    QueueUrl=self.queue_url,
                    MessageBody=self._store_sqs_s3(message),
                    **kwargs,
                )
        return response

    def send_message_batch(self, entries):
        return self._run_batch(entries, "send_message_batch")

    def delete_message_batch(self, entries):
        return self._run_batch(entries, "delete_message_batch")

    def send_back_unprocessed(self, unprocessed, delay=300):
        entries = []
        message_attrs_keys = {
            "stringValue": "StringValue",
            "binaryValue": "BinaryValue",
            "dataType": "DataType",
        }
        for record in unprocessed:
            entry = {"MessageBody": record["body"], "Id": str(uuid4())}
            if "messageAttributes" in record:
                attrs = {}
                for k, v in record["messageAttributes"].items():
                    attr_values = {}
                    for x, y in v.items():
                        if x in message_attrs_keys:
                            attr_values[message_attrs_keys[x]] = y
                    attrs[k] = attr_values
                entry["MessageAttributes"] = attrs
            if "MessageGroupId" in record["attributes"]:
                entry["MessageGroupId"] = record["attributes"]["MessageGroupId"]
            else:
                entry["DelaySeconds"] = delay
            entries.append(entry)
        self.send_message_batch(entries)
