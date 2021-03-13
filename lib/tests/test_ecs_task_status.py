import boto3
from super_user.lib.tests.utils import *
from botocore.stub import Stubber
from super_user.lib.task_runner.context.ecs.status import ECSRunContextTaskStatus, ECSStatus
import datetime
from dateutil.tz import tzlocal
import copy
import time
import contextlib
import mock
from unittest import TestCase



BASE_ARN = "arn:aws:ecs:us-west-2:408750594584:task/cluster-prod/{}"
BASE_TASK_FIXTURE = {
            u'launchType': u'FARGATE',
            u'clusterArn': u'arn:aws:ecs:us-west-2:408750594584:cluster/cluster-prod',
            u'desiredStatus': u'RUNNING',
            u'createdAt': datetime.datetime(2021, 3, 10, 13, 41, 35, 242000, tzinfo=tzlocal()),
            u'taskArn': u'arn:aws:ecs:us-west-2:408750594584:task/cluster-prod/89bb0e82f437457fbd4e5e143002cd84',
            u'group': u'family:atlantis', u'pullStartedAt': datetime.datetime(2021, 3, 10, 13, 42, 16, 109000, tzinfo=tzlocal()),
            u'version': 3, u'memory': u'512', u'connectivityAt': datetime.datetime(2021, 3, 10, 13, 41, 41, 32000, tzinfo=tzlocal()),
            u'startedAt': datetime.datetime(2021, 3, 10, 13, 42, 41, 109000, tzinfo=tzlocal()),
            u'taskDefinitionArn': u'arn:aws:ecs:us-west-2:408750594584:task-definition/atlantis:19',
            u'availabilityZone': u'us-west-2a',
            u'lastStatus': u'RUNNING',
            u'connectivity': u'CONNECTED',
            u'healthStatus': u'UNKNOWN',
        }


class TaskStartMeta(object):
    def __init__(self, age, status=ECSStatus.RUNNING):
        self.age = age
        self.start_time = time.time() - age
        self.status = status
        self.dt = datetime.datetime.fromtimestamp(self.start_time, tzlocal())

    def __repr__(self):
        return "TaskStartMeta(Age: {}, Status: {}, start_time: {})".format(self.age, self.status, self.start_time)


class TestEcsStatus(TestCase):

    def setUp(self):
        super(TestEcsStatus, self).setUp()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _task_tags(self, env_id):
        return self._tags(env_id, 'key', 'value')

    def _tags(self, env_id, key='Key', value='Value'):
        return [{key: 'env_id', value: env_id}, {key: 'kind', value: 'webscript_env'}]

    def generate_mock_task(self, start_time, status, env_id):
        mock_task = copy.copy(BASE_TASK_FIXTURE)
        mock_task['createdAt'] = datetime.datetime.fromtimestamp(start_time, tzlocal())
        mock_task['lastStatus'] = status.value
        mock_task['taskArn'] = BASE_ARN.format(uuid.uuid4())
        mock_task['tags'] = self._task_tags(env_id)
        return mock_task

    def generate_task_response(self, tasks):
        return {u'failures': [], u'tasks':tasks}

    def generate_resource_response_for_tasks(self, tasks, env_id):
        tags = self._tags(env_id)
        return {
            u'PaginationToken': u'',
            u'ResourceTagMappingList': [{u'ResourceARN': task['taskArn'], u'Tags': tags} for task in tasks]
        }


    def stubbers_from_tasks(self, tasks, env_id):
        ecs_client = boto3.client('ecs', region_name='us-west-2')
        resource_tagging_client = boto3.client('resourcegroupstaggingapi', region_name='us-west-2')
        ecs_stubber = Stubber(ecs_client)
        resource_stubber = Stubber(resource_tagging_client)
        expected_tags_params = dict(
            TagFilters=[
                {'Key': key, 'Values': [value]} for key, value in self._tags(env_id)],
            ResourceTypeFilters=[
                'ecs:task',
            ])
        expected_tags_response = self.generate_resource_response_for_tasks(tasks, env_id)
        resource_stubber.add_response('get_resources', expected_params=expected_tags_params, service_response=expected_tags_response)

        if tasks:
            ecs_expected_params = ecs_expected_params = dict(cluster=tasks[0]['clusterArn'].split("/")[-1], tasks=[task['taskArn'] for task in tasks])
            ecs_expected_response = self.generate_task_response(tasks)
            ecs_stubber.add_response('describe_tasks', service_response=ecs_expected_response, expected_params=ecs_expected_params)

        return resource_stubber, ecs_stubber

    @contextlib.contextmanager
    def task_context(self, tasks, env_id):
        resource_stubber, ecs_stubber = self.stubbers_from_tasks(tasks, env_id)
        resource_stubber.activate()
        ecs_stubber.activate()
        yield resource_stubber.client, ecs_stubber.client
        ecs_stubber.deactivate()
        resource_stubber.deactivate()

    def _test_num_available_tasks(self, task_start_metas=[], env_id="Foo", task_expiry_age=2400, expected_available=0):
        tasks = [self.generate_mock_task(start_time=task_meta.start_time, status=task_meta.status, env_id=env_id) for task_meta in task_start_metas]
        tags = self._tags(env_id)
        self.logger.info("Tasks: {}".format(task_start_metas))
        self.logger.info("With expiry of {}, {} tasks are expected to be available".format(task_expiry_age, expected_available))

        with self.task_context(tasks, env_id) as (resource_tagging_client, ecs_client):
            status = ECSRunContextTaskStatus(tags=tags, resource_tagging_client=resource_tagging_client, ecs_client=ecs_client, task_expiry_age=task_expiry_age)
            self.assertEqual(expected_available, status.num_available_tasks)

    def test_non_running_tasks_are_not_available(self):
        tasks = [TaskStartMeta(0, status) for status in [ECSStatus.DEPROVISIONING, ECSStatus.DEPROVISIONING, ECSStatus.STOPPED, ECSStatus. STOPPING]]
        self._test_num_available_tasks(tasks, expected_available=0)

    def test_running_tasks_are_available(self):
        tasks = [TaskStartMeta(0, status) for status in [ECSStatus.RUNNING, ECSStatus.PENDING, ECSStatus.PROVISIONING, ECSStatus.ACTIVATING]]
        self._test_num_available_tasks(tasks, expected_available=4)

    def test_none_available(self):
        tasks = []
        self._test_num_available_tasks(tasks, expected_available=0)

    def test_old_tasks_are_not_available(self):
        tasks = [TaskStartMeta(age, ECSStatus.RUNNING) for age in [0, 600, 1800, 2500, 3000]]
        self._test_num_available_tasks(tasks, expected_available=5, task_expiry_age=4000)
        self._test_num_available_tasks(tasks, expected_available=3, task_expiry_age=2000)
        self._test_num_available_tasks(tasks, expected_available=2, task_expiry_age=1400)

    def test_context_refreshes_if_stale(self):

        def test_staleness_refresh(staleness, refresh_frequency=30):
            status = ECSRunContextTaskStatus(None, None, refresh_frequency=refresh_frequency)
            status.staleness_in_seconds = lambda: staleness
            with mock.patch.object(status, '_update_task_status') as task_status_call:
                status.task_status
                self.logger.info("Staleness: {}, Max staleness: {}, Checking if status refreshed...".format(staleness, refresh_frequency))
                if staleness > refresh_frequency:
                    task_status_call.assert_called_once()
                    self.logger.info("Refresh called. Success!")
                else:
                    task_status_call.assert_not_called()
                    self.logger.info("Refresh not called. Success!")

        test_staleness_refresh(0)
        test_staleness_refresh(20)
        test_staleness_refresh(60)
