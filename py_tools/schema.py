from jsonschema import Draft7Validator

from py_tools import format


def validate_schema_data(incoming_data, json_schema):
    incoming_data = format.clean_empty(incoming_data)
    if not Draft7Validator(json_schema).is_valid(incoming_data):
        raise BaseException
    return {"content": incoming_data, "schema": json_schema}


def validate_pages_schema_data(incoming_data, json_schema):
    page_errors = []
    for index, page in enumerate(incoming_data):
        page = format.clean_empty(page)
        try:
            Draft7Validator(json_schema[index]["schema"]).validate(page)
        except BaseException as e:
            msg = "{}.{}:{}".format(index, ".".join(e.absolute_schema_path), e.message)
            page_errors.append(msg)

    if page_errors:
        raise Exception(",".join(page_errors))
    return {"page_content": incoming_data, "page_schema": json_schema}
