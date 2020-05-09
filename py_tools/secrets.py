import os

import boto3

from py_tools import format

ssm = boto3.client('ssm')
secretsmanager = boto3.client('secretsmanager')


def replace_value(value):
    if value == 'true':
        return True
    elif value == 'false':
        return False
    return value


def get_parameters(path=None, names=None, load=False):
    if names:
        response = ssm.get_parameters(
            Names=names,
            WithDecryption=True
        )
    else:
        response = ssm.get_parameters_by_path(
            Path=path,
            WithDecryption=True
        )
    output = {}
    for parameter in response['Parameters']:
        output[parameter['Name'].split('/')[-1]] = parameter['Value']
    if load:
        print('Adding parameters into env...')
        for k, v in output.items():
            os.environ[k] = v
    output = {k: replace_value(v) for k, v in output.items()}
    return output


def load_secret_manager(secret_name, names=None, load=True):
    secrets = secretsmanager.get_secret_value(SecretId=secret_name)['SecretString']
    secrets = format.loads(secrets)
    if names:
        secrets = {k: v for k, v in secrets.items() if k in names}
    if load:
        print('Adding secrets into env...')
        for key, value in secrets.items():
            if key not in os.environ:
                os.environ[key] = value
    secrets = {k: replace_value(v) for k, v in secrets.items()}
    return secrets
