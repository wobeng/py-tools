import boto3
import time

client = boto3.client("logs")


def get_events(log_group, log_stream):
    logs = []
    kwargs = {
        "logGroupName": log_group,
        "logStreamName": log_stream,
        "limit": 10000,
    }
    while True:
        resp = client.get_log_events(**kwargs)
        logs.extend(resp["events"])
        try:
            kwargs["nextToken"] = resp["nextToken"]
        except KeyError:
            break
    return logs


def put_events(log_group, log_stream, messages):
    timestamp = int(round(time.time() * 1000))
    client.create_log_stream(
        logGroupName=log_group,
        logStreamName=log_stream,
    )
    client.put_log_events(
        logGroupName=log_group,
        logStreamName=log_stream,
        logEvents=[{"timestamp": timestamp, "message": m} for m in messages],
    )
