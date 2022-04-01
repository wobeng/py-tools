import os

import boto3

from py_tools import format

ssm = boto3.client("ssm")
secretsmanager = boto3.client("secretsmanager")


def replace_value(value):
    if value == "true":
        return True
    elif value == "false":
        return False
    return value


def get_parameters(path=None, names=None, load=False, ssm_client=ssm):
    if names:
        response = ssm_client.get_parameters(Names=names, WithDecryption=True)
    else:
        response = ssm_client.get_parameters_by_path(Path=path, WithDecryption=True)
    output = {}
    for parameter in response["Parameters"]:
        output[parameter["Name"].split("/")[-1]] = parameter["Value"]
    if load:
        for k, v in output.items():
            os.environ[k] = v
    output = {k: replace_value(v) for k, v in output.items()}
    return output


def load_secret_manager(
    secret_names,
    names=None,
    load=True,
    secrets_client=secretsmanager,
    force=False,
):
    secrets = {}
    for secret_name in secret_names.split(","):
        secret = secrets_client.get_secret_value(SecretId=secret_name)["SecretString"]
        secret = format.loads(secret)
        secrets.update(secret)
    if names:
        secrets = {k: v for k, v in secrets.items() if k in names}
    if load:
        for key, value in secrets.items():
            if force:
                os.environ[key] = value
            else:
                if key not in os.environ:
                    os.environ[key] = value

    secrets = {k: replace_value(v) for k, v in secrets.items()}
    return secrets
