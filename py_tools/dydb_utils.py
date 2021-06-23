import re

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer


def get_all_items(table, attributes_to_get=None):
    last_evaluated_key = None
    items = []

    while True:
        results = table.scan(
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get
        )
        for item in results:
            items.append(item)

        if results.last_evaluated_key is None:
            break

        last_evaluated_key = results.last_evaluated_key

    return items


class StreamRecord:

    @staticmethod
    def deserialize_output(value):
        try:
            td = TypeDeserializer()
            for k, v in dict(value).items():
                value[k] = td.deserialize(v)
        except BaseException:
            pass
        return value

    @staticmethod
    def serialize_output(value):
        try:
            td = TypeSerializer()
            for k, v in dict(value).items():
                value[k] = td.serialize(v)
        except BaseException:
            pass
        return value

    def __init__(self, record):
        self.key = self.deserialize_output(record['dynamodb']['Keys'])
        self.new_image = self.deserialize_output(record['dynamodb'].get('NewImage', {}))
        self.old_image = self.deserialize_output(record['dynamodb'].get('OldImage', {}))
        self.event_name = record['eventName'].lower()
        self.table_name = record['eventSourceARN'].split('/')[-3]
        self.trigger_module = '_'.join(re.findall('[A-Z][^A-Z]*', self.table_name)).lower()
        if record.get('userIdentity', {}).get('type', '') == 'Service':
            self.ttl = True
        else:
            self.ttl = False
