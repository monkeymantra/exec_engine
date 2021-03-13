import time

from super_user.lib.task_runner.context.status import RunContextTaskStatus


class ProcessRunContextTaskStatus(RunContextTaskStatus):
    def __init__(self, **kwargs):
        super(ProcessRunContextTaskStatus, self).__init__(**kwargs)

    def check_alive(self, process):
        return not process.poll()