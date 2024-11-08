from dotenv import load_dotenv
import os

import boto3

from py_tools import format

ssm = boto3.client("ssm")


def environ_wrap(value):
    if isinstance(value, bool):
        return str(value).lower()
    return value


def get_parameters(caller_file, path=None, names=None, load=False, ssm_client=ssm):
    load_env(caller_file)
    if names:
        response = ssm_client.get_parameters(Names=names, WithDecryption=True)
    else:
        response = ssm_client.get_parameters_by_path(Path=path, WithDecryption=True)
    output = {}
    for parameter in response["Parameters"]:
        output[parameter["Name"].split("/")[-1]] = parameter["Value"]
    if load:
        for k, v in output.items():
            os.environ[k] = environ_wrap(v)
    output = {k: v for k, v in output.items()}
    return output


def load_env(caller_file):
    # Get the directory of the caller's script
    caller_directory = os.path.dirname(os.path.abspath(caller_file))

    # Construct the path to the .env file in the caller's directory
    env_path = os.path.join(caller_directory, ".env")

    # Load the .env file from the caller's directory
    load_dotenv(env_path, override=True)


def load_secret_manager(
    caller_file,
    secret_names,
    names=None,
    load=True,
    secrets_client=None,
    force=False,
):
    load_env(caller_file)

    secrets_client = secrets_client or boto3.Session().client("secretsmanager")
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
                os.environ[key] = environ_wrap(value)
            else:
                if key not in os.environ:
                    os.environ[key] = environ_wrap(value)

    secrets = {k: v for k, v in secrets.items()}

    return secrets
