from jsonschema import Draft7Validator

from py_tools import format


def validate_schema_data(incoming_data, json_schema):
    incoming_data = format.clean_empty(incoming_data)
    if not Draft7Validator(json_schema).is_valid(incoming_data):
        raise BaseException
    return {'content': incoming_data, 'schema': json_schema}


def validate_pages_schema_data(incoming_data, json_schema):
    page_schema = []
    page_content = []
    for index, page in enumerate(incoming_data):
        page = format.clean_empty(page)
        if not Draft7Validator(json_schema[index]['schema']).is_valid(page):
            raise BaseException
        page_content.append(page)
        page_schema.append(json_schema[index]['schema'])
    return {'page_content': page_content, 'page_schema': page_schema}
