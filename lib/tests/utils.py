import uuid
from queue import Queue
from super_user.lib.task_runner.sinks import EventSink
from super_user.lib.task_runner.source import WorkSource
from super_user.lib.task_runner.models import RunCommand, RunResult, RunResultSchema
import logging

ENV_ID="FOO"
logging.basicConfig(level=logging.INFO)


class MockWorkSource(WorkSource):
    def get_work(self):
        return uuid.uuid4(), RunCommand(env_id=ENV_ID, run_id=uuid.uuid4(), run_by="Greg")


class MockEventSink(EventSink):
    def __init__(self, **kwargs):
        if not kwargs:
            kwargs = dict(encoders={RunResult: RunResultSchema.message_handler()})
        super(MockEventSink, self).__init__(**kwargs)
        self.events = Queue()

    def _send_event(self, event, event_type):
        print event
        self.events.put(event)


def handler(run_command):
    run_id = run_command.run_id
    return RunResult(run_id, "bar", "baz")
