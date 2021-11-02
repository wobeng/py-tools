import importlib.util
import os
from py_tools.dydb_utils import StreamRecord
import traceback
from py_tools.format import loads


class Handlers:
    def __init__(self, file, record, context, record_wrapper=None):
        self.file = file
        self.record = record
        self.context = context
        self.record_wrapper = record_wrapper

    def dynamodb(self):
        wrapper = self.record_wrapper or StreamRecord
        record = wrapper(self.record)
        m = self.module_handler(
            self.file, record.trigger_module, folder='dynamodb')
        functions = getattr(m, record.event_name, [])
        for function in functions:
            function(record, self.context)
        return

    def sqs(self):
        module_name = self.record['eventSourceARN'].split(
            ':')[-1].replace('.fifo', '')
        m = self.module_handler(self.file, module_name, folder='sqs')
        return m.handler(loads(self.record['body']), self.record)

    def adhoc(self):
        m = self.module_handler(self.file, self.record['type'], folder='adhoc')
        return m.handler(self.record)

    @staticmethod
    def module_handler(file, module_name, folder='sqs'):
        path = os.path.dirname(os.path.realpath(file)) + \
            '/{}/{}.py'.format(folder, module_name)
        name = path.split('/')[-1].replace('.py', '')
        spec = importlib.util.spec_from_file_location(name, path)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m


def aws_lambda_handler(file, record_wrapper=None):
    def handler(event, context):
        processed, unprocessed = [], []
        for record in event['Records']:
            source_handler = record['eventSource'].split(
                ':')[-1].lower()  # should be dynamodb, sqs or adhoc
            try:
                method = getattr(
                    Handlers(file, record, context, record_wrapper), source_handler)
                processed.append(method())  # run and add to process list
            except BaseException:
                unprocessed.append(record)
                traceback.print_exc()
        # send back unprocessed later
        return processed
    return handler
