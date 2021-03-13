from status.process import ProcessRunContextTaskStatus
from super_user.lib.task_runner.context import RunContext
import subprocess
import shlex


class ProcessRunContext(RunContext):
    def __init__(self, *args, **kwargs):
        super(ProcessRunContext, self).__init__(*args, **kwargs)
        import os
        self.env = kwargs.get('env', os.environ)
        self.status = ProcessRunContextTaskStatus()

    def _launch_backend(self):
        process = subprocess.Popen(shlex.split(self._start_command()), env=self.env)
        self.status.add_task(process)
