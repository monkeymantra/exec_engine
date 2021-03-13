from __future__ import absolute_import

from super_user.lib.tests.utils import *
from super_user.lib.task_runner import TaskRunner
from super_user.lib.task_runner.models import RunCommand
import time


from unittest import TestCase


class TaskRunnerTest(TestCase):

    def setUp(self):
        super(TaskRunnerTest, self).setUp()

    def test_run_simple_tasks(self):
        event_sink = MockEventSink()
        runner = TaskRunner(True, time.time(), max_age=10, work_source=MockWorkSource(), event_sink=event_sink, kill_after=10)
        runner.register_handler(RunCommand, handler)
        runner.start()
        assert event_sink.events.qsize() == 10