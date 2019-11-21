import os

from simplejson import loads


def load_secret(secrets):
    if isinstance(secrets, str):
        secrets = loads(secrets)
    secrets = secrets or {}
    for key, value in secrets.items():
        if key not in os.environ:
            os.environ[key] = value
    return secrets
