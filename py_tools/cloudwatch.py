import boto3

client = boto3.client('logs')


def get_events(log_group, log_stream):
    logs = []
    kwargs = {
        'logGroupName': log_group,
        'logStreamName': log_stream,
        'limit': 10000
    }
    while True:
        resp = client.get_log_events(**kwargs)
        logs.extend(resp['events'])
        try:
            kwargs['nextToken'] = resp['nextToken']
        except KeyError:
            break
    return logs
