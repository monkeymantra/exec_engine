import boto3
import logging
import enum
import json
from models import RunCommandSchema, RunCommand


class SourceType(str, enum.Enum):
    sqs = "sqs"
    zmq = "zmq"
    null = "null"
    mock = "mock"


class WorkSource(object):
    def get_work(self):
        pass

    def ack_work(self, work_id):
        pass


class SQSWorkSource(WorkSource):
    def __init__(self, sqs_queue_name, timeout=1):
        self.client = boto3.client('sqs')
        self.timeout = timeout
        self.queue_url = self._queue_url(sqs_queue_name)
        self.message_schema = RunCommandSchema()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _queue_url(self, queue_name):
        return self.client.get_queue_url(QueueName=queue_name)['QueueUrl']

    def get_work(self):
        messages = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=self.timeout,
        ).get('Messages')
        if messages:
            print messages
            message = messages[0]
            work_id = message['ReceiptHandle']
            body = json.loads(messages[0]['Body'])['Message']
            work = self.message_schema.loads(body)
            return work_id, self.message_schema.loads(body).data
        else:
            return None, None

    def ack_work(self, work_id):
        try:
            self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=work_id)
        except Exception as e:
            self.logger.error("Unable to delete work message", exc_info=e)


class SourceFactory(object):
    @staticmethod
    def get_source(url):
        source_type, source_url = url.split("://")
        if source_type == SourceType.sqs:
            return SQSWorkSource(source_url)
