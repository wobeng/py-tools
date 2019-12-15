from simplejson import dumps
import boto3
import traceback

sqs = boto3.client('sqs')
sts = boto3.client('sts')

class Sns:
    def __init__(self,group_name=None, region_name=None):
        self.client = sqs
        self.account_id = sts.get_caller_identity()['Account']
        self.region_name = sqs.meta.region_name
        if group_name:
            self.group_name = group_name
        if region_name:
            self.region_name = region_name

    def publish(self, subject, message):
        topic_arn = 'arn:aws:sns:{}:{}:{}'.format(self.region_name, self.account_id, self.group_name)
        self.client.publish(TopicArn=topic_arn, Message=message, Subject=subject)

    def send_exception_email(self, domain, event):
        message = dumps(event) + '\n\n\n' + traceback.format_exc()
        subject = 'Error occurred in ' + domain
        self.publish(subject, message)
