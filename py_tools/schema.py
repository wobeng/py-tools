from jsonschema import Draft7Validator

from py_tools import format


def validate_schema_data(incoming_data, json_schema):
    incoming_data = format.clean_empty(incoming_data)
    if not Draft7Validator(json_schema).is_valid(incoming_data):
        raise BaseException
    return {"content": incoming_data, "schema": json_schema}


def validate_schemas_data(incoming_data, json_schema):
    type_errors = []
    
    for index, item in enumerate(incoming_data):
        item = format.clean_empty(item)
        try:
            Draft7Validator(json_schema[index]["schema"]).validate(item)
        except BaseException as e:
            msg = "{}.{}:{}".format(index, ".".join(
                e.absolute_schema_path), e.message)
            type_errors.append(msg)

    if type_errors:
        raise Exception(",".join(type_errors))
    return {"content": incoming_data,  "schema": json_schema}
