import os

import boto3

from py_tools import format

secretsmanager = boto3.client('secretsmanager')


def load_secret(secrets):
    if isinstance(secrets, str):
        secrets = format.loads(secrets)
    secrets = secrets or {}
    for key, value in secrets.items():
        if key not in os.environ:
            os.environ[key] = value
    return secrets


def load_secret_manager(secret_name):
    print('Loading secrets...')
    return load_secret(secretsmanager.get_secret_value(SecretId=secret_name)['SecretString'])
