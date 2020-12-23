import boto3
from boto3.session import Session

sts = boto3.client('sts')


def assume_role(role_name, session_name, sts_client=sts):
    response = sts_client.assume_role(RoleArn=role_name, RoleSessionName=session_name)
    session = Session(
        aws_access_key_id=response['Credentials']['AccessKeyId'],
        aws_secret_access_key=response['Credentials']['SecretAccessKey'],
        aws_session_token=response['Credentials']['SessionToken']
    )
    return session
