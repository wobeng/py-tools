import os

import boto3

from py_tools import format

ssm = boto3.client('ssm')
secretsmanager = boto3.client('secretsmanager')


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
        print('Loading parameters...')
        for k, v in output.items():
            os.environ[k] = v
    return output


def load_secret_manager(secret_name):
    print('Loading secrets...')
    secrets = secretsmanager.get_secret_value(SecretId=secret_name)['SecretString']
    secrets = format.loads(secrets)
    for key, value in secrets.items():
        if key not in os.environ:
            os.environ[key] = value
    return secrets