import unittest

from super_user.lib.task_runner.sinks import *
from super_user.lib.task_runner.context.thread import ThreadRunContext
import mock
from super_user.lib.tests.utils import *
from unittest import TestCase
from moto import mock_sns, mock_sqs
import boto3
import json
import time

def allow_sns_to_write_to_sqs(topicarn, queuearn):
    policy_document = """{{
  "Version":"2012-10-17",
  "Statement":[
    {{
      "Sid":"MyPolicy",
      "Effect":"Allow",
      "Principal" : {{"AWS" : "*"}},
      "Action":"SQS:SendMessage",
      "Resource": "{}",
      "Condition":{{
        "ArnEquals":{{
          "aws:SourceArn": "{}"
        }}
      }}
    }}
  ]
}}""".format(queuearn, topicarn)
    return policy_document

def filter_policy(env_id):
    return json.dumps({
        'env_id': [env_id],
        'event_type': ['RunCommand']
    })

@mock_sns
@mock_sqs
class TestAwsFlow(TestCase):

    def setUp(self):
        super(TestAwsFlow, self).setUp()
        self.sns = boto3.client('sns')
        self.sqs = boto3.client('sqs')

    def _parse_sqs_results(self, results):
        for result in results['Messages']:
            body = json.loads(result['Body'])['Message']
            yield RunResultSchema().loads(body).data

    def get_queue_arn(self, queue_url):
        sqs_queue_attrs = self.sqs.get_queue_attributes(QueueUrl=queue_url,
                                                   AttributeNames=['All'])['Attributes']
        sqs_queue_arn = sqs_queue_attrs['QueueArn']
        if ':sqs.' in sqs_queue_arn:
            sqs_queue_arn = sqs_queue_arn.replace(':sqs.', ':')
        return sqs_queue_arn

    def test_submit_work(self):
        sqs_queue_name = "my_env"
        sns_topic_name = "results"
        sqs_results = "results"

        topic = self.sns.create_topic(Name="work")
        topic_arn = topic.get('TopicArn')

        results_topic = self.sns.create_topic(Name="results")
        results_topic_arn = results_topic.get('TopicArn')

        work_queue = self.sqs.create_queue(QueueName=sqs_queue_name)
        results_queue = self.sqs.create_queue(QueueName=sqs_results)

        queue_url = work_queue.get('QueueUrl')
        results_queue_url = results_queue.get('QueueUrl')

        sqs_queue_arn = self.get_queue_arn(queue_url)
        results_queue_arn = self.get_queue_arn(results_queue_url)

        policy = allow_sns_to_write_to_sqs(topic_arn, sqs_queue_arn)
        self.sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={'Policy': policy})
        self.sqs.set_queue_attributes(QueueUrl=results_queue_url, Attributes={'Policy': allow_sns_to_write_to_sqs(results_topic_arn, results_queue_arn)})

        subscription = self.sns.subscribe(
            TopicArn=topic_arn,
            Protocol='sqs',
            Endpoint=sqs_queue_arn,
        )

        results_subscription = self.sns.subscribe(
            TopicArn=results_topic_arn,
            Protocol='sqs',
            Endpoint=results_queue_arn,
        )

        subscription_arn = subscription['SubscriptionArn']
        results_subscription_arn = results_subscription['SubscriptionArn']

        self.sns.set_subscription_attributes(SubscriptionArn=subscription_arn, AttributeName='FilterPolicy', AttributeValue=filter_policy("Foo"))

        event_sink = SNSEventSink(topic_arn)
        env_id = "Foo"
        command = RunCommand(env_id=env_id, run_id="1", run_by="grice")
        context = ThreadRunContext(env_id, event_sink, source_url="sqs://" + sqs_queue_name, result_url="sns://" + results_topic_arn, num_desired_tasks=1)
        context.submit(command)
        context.submit(RunCommand(env_id=env_id, run_id="4", run_by="grice"))
        time.sleep(2)
        results = self._parse_sqs_results(self.sqs.receive_message(QueueUrl=results_queue_url, MaxNumberOfMessages=10))
        r = list(results)
        self.assertEqual(2, len(r))


