from jsonschema import Draft7Validator

from py_tools import format


def validate_schema_data(incoming_data, json_schema):
    print(incoming_data, type(incoming_data))
    print(json_schema, type(json_schema))
    incoming_data = format.clean_empty(incoming_data)
    if not Draft7Validator(json_schema).is_valid(incoming_data):
        raise BaseException
    return {'content': incoming_data, 'schema': json_schema}
