from super_user.lib.task_runner.context.config import RunContextConfig
from status import RunContextTaskStatus
from super_user.lib.task_runner.context.status.process import ProcessRunContextTaskStatus
from super_user.lib.task_runner.context.status.thread import ThreadRunContextTaskStatus
import logging
import subprocess
import shlex
import threading


class RunContext(object):
    def __init__(self, env_id, event_sink, *args, **kwargs):
        self.env_id = env_id
        self.event_sink = event_sink
        self.source_url = kwargs['source_url']
        self.result_url = kwargs['result_url']
        self.task_expiry_age = kwargs.get('task_expiry_age', 2400)
        self.num_desired_tasks = kwargs.get('num_desired_tasks', 1)
        self._killed = False
        self.logger = logging.getLogger(str(self))
        self.status = RunContextTaskStatus()

    def _start_command(self):
        return "python manage.py launch_context_runner --source {} --sink {} --max-age {}".format(
            self.source_url,
            self.result_url,
            self.task_expiry_age)

    @property
    def id(self):
        return self.env_id

    def name(self):
        pass

    def kill(self):
        self._killed = True
        return self._kill()

    def _kill(self):
        for task in self.status.active_workers:
            try:
                self._stop_task(task)
            except Exception as e:
                self.logger.error("Failed to stop task...", e)

    def _stop_task(self, task):
        return True

    @property
    def killed(self):
        return self._killed

    def _launch_backend(self):
        pass

    def refresh(self):
        tasks_needed = self.num_desired_tasks - self.status.num_available_tasks
        for x in range(tasks_needed):
            self._launch_backend()

    def submit(self, event):
        self.refresh()
        self.event_sink.send_event(event, env_id=self.env_id)

    def ready(self):
        return True

    @property
    def needs_refresh(self):
        return False

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.env_id)

