import mock
from super_user.lib.tests.utils import *
from unittest import TestCase
from super_user.lib.task_runner.sinks import SNSEventSink
from super_user.lib.task_runner.models import RunCommand
import boto3
import moto
from moto import mock_sns

class TestSNSEventSink(TestCase):

    @mock_sns
    def setUp(self):
        super(TestSNSEventSink, self).setUp()
        self.sns_client = boto3.client('sns')

    @mock_sns
    def test_sns_event_sink_publishes_command(self):
        topic = 'commands'
        sink = SNSEventSink(topic)
        expected_publish_params = {}
        sns = boto3.client('sns')
        topic = sns.create_topic(Name=topic)
        arn = topic.get('TopicArn')
        sink = SNSEventSink(arn)
        command = RunCommand(env_id="Foo", run_id="1", run_by="grice")
        sink.send_event(command)