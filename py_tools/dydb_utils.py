import re

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from decimal import Decimal

def deserialize_output(value):
    td = TypeDeserializer()
    try:
        for k, v in dict(value).items():
            py_val = td.deserialize(v)
            if isinstance(py_val, Decimal):
                py_val = int(py_val)
            value[k] = py_val
    except BaseException:
        pass
    return value

def serialize_input(value):
    td = TypeSerializer()
    try:
        for k, v in dict(value).items():
            value[k] = td.serialize(v)
    except BaseException:
        pass
    return value

def get_all_items(table, attributes_to_get=None):
    last_evaluated_key = None
    items = []

    while True:
        results = table.scan(
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
        )
        for item in results:
            items.append(item)

        if results.last_evaluated_key is None:
            break

        last_evaluated_key = results.last_evaluated_key

    return items


class StreamRecord:

    def __init__(self, record):
        self.key = deserialize_output(record["dynamodb"]["Keys"])
        self.new_image = deserialize_output(
            record["dynamodb"].get("NewImage", {})
        )
        self.old_image = deserialize_output(
            record["dynamodb"].get("OldImage", {})
        )
        self.event_name = record["eventName"].lower()
        self.table_name = record["eventSourceARN"].split("/")[-3]
        self.trigger_module = "_".join(
            re.findall("[A-Z][^A-Z]*", self.table_name)
        ).lower()
        if record.get("userIdentity", {}).get("type", "") == "Service":
            self.ttl = True
        else:
            self.ttl = False
