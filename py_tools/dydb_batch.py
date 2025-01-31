import random
import time
import boto3
from py_tools.dydb_utils import deserialize_output, serialize_input
from py_tools.pylog import get_logger
from py_tools.format import dumps


dynamodb_client = boto3.client("dynamodb")
dynamodb_resource = boto3.resource("dynamodb")
logger = get_logger("py-tools.dydb_batch")


def projection_string(func):
    def wrapper(*args, **kwargs):
        if "ProjectionExpression" in kwargs:
            names = {}
            counter = 1
            attributes = kwargs["ProjectionExpression"].split(",")
            for a_index, attribute in enumerate(attributes):
                sub_attributes = attribute.split(".")
                for sa_index, sub_attribute in enumerate(sub_attributes):
                    place_holder = "#attr" + str(counter)
                    names[place_holder] = sub_attribute
                    sub_attributes[sa_index] = place_holder
                    counter += 1
                attribute = ".".join(sub_attributes)
                attributes[a_index] = attribute
            kwargs["ProjectionExpression"] = ",".join(attributes)
            kwargs["ExpressionAttributeNames"] = names
        return func(*args, **kwargs)

    return wrapper


class DynamoDbBatch:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        self.client = dynamodb_client
        self.resource_client = dynamodb_resource
        self.request_items = {}
        self.get_items_keys = {}
        self.get_serialized_items = {}

    @projection_string
    def get_item(self, table, key, **kwargs):
        if table not in self.get_items_keys:
            self.get_items_keys[table] = set()
            self.get_serialized_items[table] = {"Keys": []}
        key_frozenset = frozenset(key.items())
        if key_frozenset not in self.get_items_keys[table]:
            # logger.debug("Queuing get item %s to table %s" % (dumps(key), table))
            self.get_items_keys[table].add(key_frozenset)
            self.get_serialized_items[table]["Keys"].append(serialize_input(key))
            self.get_serialized_items[table].update(kwargs)

    def post_item(self, table, item):
        if table not in self.request_items:
            self.request_items[table] = []
        logger.debug("Queuing put item %s to table %s" % (dumps(item), table))
        self.request_items[table].append({"put_item": {"Item": item}})

    def delete_item(self, table, key):
        if table not in self.request_items:
            self.request_items[table] = []
        logger.debug("Queuing delelte item %s to table %s" % (dumps(key), table))
        self.request_items[table].append({"delete_item": {"Key": key}})

    def batch_read(self):
        n = 0
        results = {}
        # serialize
        response = self.client.batch_get_item(RequestItems=self.get_serialized_items)
        results.update(response["Responses"])
        while response["UnprocessedKeys"]:
            # Implement some kind of exponential back off here
            n = n + 1
            time.sleep((2**n) + random.randint(0, 1000) / 1000)
            response = self.client.batch_get_item(
                RequestItems=response["UnprocessedKeys"]
            )
            results.update(response["Responses"])
        for table, records in dict(results).items():
            results[table] = [deserialize_output(r) for r in records]
        return results

    def batch_write(self):
        table_ct, delete_ct, put_ct = 0, 0, 0
        for table_name, records in self.request_items.items():
            table_ct += 1
            table = self.resource_client.Table(table_name)
            with table.batch_writer() as batch:
                for record in records:
                    if "put_item" in record:
                        put_ct += 1
                        item = record["put_item"]["Item"]
                        logger.debug(
                            "Adding item %s to table %s" % (dumps(item), table_name)
                        )
                        if not self.dry_run:
                            batch.put_item(Item=item)
                    elif "delete_item" in record:
                        delete_ct += 1
                        key = record["delete_item"]["Key"]
                        logger.debug(
                            "Deleting key %s from table %s" % (dumps(key), table_name)
                        )
                        if not self.dry_run:
                            batch.delete_item(Key=key)
        logger.debug(
            "Wrote %s items and deleted %s items from %s tables"
            % (put_ct, delete_ct, table_ct)
        )
