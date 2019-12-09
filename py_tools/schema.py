import json

from jsonschema import Draft7Validator

from py_tools.format import clean_empty


def validate_schema_data(incoming_data, json_schema):
    incoming_data = clean_empty(incoming_data)
    if not Draft7Validator(json.loads(json_schema)).is_valid(incoming_data):
        raise BaseException
    return {'content': incoming_data, 'schema': json_schema}
