import random
import time

import boto3
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

dynamodb = boto3.client("dynamodb")


def serialize_input(value):
    output = {}
    ty = TypeSerializer()
    for k, v in value.items():
        output[k] = ty.serialize(v)
    return output


def deserialize_output(value):
    ty = TypeDeserializer()
    for k, v in dict(value).items():
        value[k] = int(v["N"]) if "N" in v else ty.deserialize(v)
    return value


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
    def __init__(self):
        self.client = dynamodb
        self.request_items = {}

    @projection_string
    def get_item(self, table, keys, **kwargs):
        kwargs["Keys"] = [serialize_input(k) for k in keys]
        self.request_items[table] = kwargs
        return self

    def post_item(self, table, item):
        if table not in self.request_items:
            self.request_items[table] = []
        self.request_items[table].append(
            {"PutRequest": {"Item": serialize_input(item)}}
        )

    def delete_item(self, table, key):
        if table not in self.request_items:
            self.request_items[table] = []
        self.request_items[table].append(
            {"DeleteRequest": {"Key": serialize_input(key)}}
        )

    def batch_read(self):
        n = 0
        results = {}
        response = self.client.batch_get_item(RequestItems=self.request_items)
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
        n = 0
        response = self.client.batch_write_item(
            RequestItems=self.request_items
        )
        while response["UnprocessedItems"]:
            # Implement some kind of exponential back off here
            n = n + 1
            time.sleep((2**n) + random.randint(0, 1000) / 1000)
            response = self.client.batch_write_item(
                RequestItems=response["UnprocessedItems"]
            )
