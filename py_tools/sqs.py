from uuid import uuid4

import boto3

from py_tools import format

sqs = boto3.client('sqs')


class Sqs:
    def __init__(self, queue_name=None, queue_url=None):
        self.client = sqs
        self.queue_url = queue_url
        if queue_name:
            self.queue_url = self.client.get_queue_url(QueueName=queue_name)['QueueUrl']

    def _run_batch(self, entries, function_name):
        n = 10
        new_entries = [entries[i * n:(i + 1) * n] for i in range((len(entries) + n - 1) // n)]

        for ne in new_entries:
            ne_copy = ne.copy()
            get_out = False
            while not get_out:
                response = getattr(self.client, function_name)(
                    QueueUrl=self.queue_url,
                    Entries=ne_copy
                )
                if response.get('Failed', None):
                    failed_ids = [f['Id'] for f in response['Failed']]
                    ne_copy = [e for e in ne if e['Id'] in failed_ids]
                else:
                    get_out = True

    def receive_messages(self, limit=10, **kwargs):
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=limit,
            **kwargs
        )
        return response

    def send_message(self, message, **kwargs):
        response = self.client.send_message(
            QueueUrl=self.queue_url,
            MessageBody=format.dumps(message, use_decimal=True),
            **kwargs
        )
        return response

    def send_message_batch(self, entries):
        return self._run_batch(entries, 'send_message_batch')

    def delete_message_batch(self, entries):
        return self._run_batch(entries, 'delete_message_batch')

    def send_back_unprocessed(self, unprocessed, delay=300):
        entries = []
        for record in unprocessed:
            entry = {
                'MessageBody': record['body'],
                'Id': str(uuid4())
            }
            if 'messageAttributes' in record:
                entry['messageAttributes'] = record['messageAttributes']
            if 'MessageGroupId' in record['attributes']:
                entry['MessageGroupId'] = record['attributes']['MessageGroupId']
            else:
                entry['DelaySeconds'] = delay

            entries.append(entry)
        self.send_message_batch(entries)
