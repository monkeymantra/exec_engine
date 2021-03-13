from __future__ import absolute_import
from super_user.lib.task_runner.context.ecs import ECSRunContext
from super_user.lib.task_runner.models import RunResultSchema
from eta.external_task_scheduler import EcsTaskExecutor
from super_user.lib.task_runner.context.ecs import ECSRunContext, ECSRunContextTaskStatus
import mock
from super_user.lib.tests.utils import *
from unittest import TestCase


class TestEcsRunContext(TestCase):

    def setUp(self):
        super(TestEcsRunContext, self).setUp()

    @mock.patch('super_user.lib.task_runner.context.ecs.ECSRunContextTaskStatus.num_available_tasks', new_callable=mock.PropertyMock)
    @mock.patch('eta.external_task_scheduler.EcsTaskExecutor')
    def test_executor_launches_when_refresh_needed(self, mock_executor, mock_num_available):
        currently_available = 0
        mock_num_available.return_value = currently_available
        ecs_status = ECSRunContextTaskStatus()
        mock_executor.run_task.return_value = 'ec3b4ec971c24e9294a3e3738a7bd9b6'
        event_sink = MockEventSink()
        run_context = ECSRunContext("Foo", event_sink, "source_url", "result_url", mock_executor, ecs_status=ecs_status)
        self.assertTrue(run_context.needs_refresh)
        run_context.refresh()
        self.assertEqual(mock_executor.run_task.call_count, run_context.num_desired_tasks - currently_available)

    @mock.patch('boto3.client')
    @mock.patch('super_user.lib.task_runner.context.ecs.ECSRunContextTaskStatus.active_workers', new_callable=mock.PropertyMock)
    @mock.patch('super_user.lib.task_runner.context.ecs.ECSRunContextTaskStatus.num_available_tasks', new_callable=mock.PropertyMock)
    @mock.patch('eta.external_task_scheduler.EcsTaskExecutor')
    def test_run_context_kills_jobs(self, mock_executor, mock_num_available, mock_active_workers, ecs_client_mock):
        tasks = [{'taskArn': 'cluster-prod/task{}'.format(n), 'clusterArn':'cluster-prod'} for n in range(4)]
        mock_num_available.return_value = 4
        mock_active_workers.return_value = tasks
        ecs_status = ECSRunContextTaskStatus()
        ecs_client_mock.stop_task.return_value = ""
        event_sink = MockEventSink()
        run_context = ECSRunContext("Foo", event_sink, "source_url", "result_url", mock_executor, ecs_client=ecs_client_mock, ecs_status=ecs_status)
        self.assertFalse(run_context.needs_refresh)
        run_context.kill()
        self.assertEqual(4, ecs_client_mock.stop_task.call_count)
