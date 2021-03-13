from status.thread import ThreadRunContextTaskStatus
from super_user.lib.task_runner.context import RunContext
from super_user.lib.task_runner.sinks import SinkFactory
from super_user.lib.task_runner.source import SourceFactory
from super_user.lib.task_runner import TaskRunner
from super_user.lib.task_runner.models import RunResult, RunCommand
import time
import threading
import uuid



def print_handler(run_command):
    run_id = run_command.run_id
    return RunResult(run_id, "0", "finished")


def run(**options):
    start_time = time.time()
    stay_alive = options['stay_alive']
    max_age = options['max_age']
    source = options['source']
    sink = options['sink']
    event_sink = SinkFactory.get_sink(sink)
    work_source = SourceFactory.get_source(source)
    runner = TaskRunner(stay_alive, start_time, max_age, event_sink=event_sink, work_source=work_source)
    runner.register_handler(RunCommand, print_handler)
    runner.start()


class ThreadRunContext(RunContext):
    def __init__(self, *args, **kwargs):
        super(ThreadRunContext, self).__init__(*args, **kwargs)
        self.status = ThreadRunContextTaskStatus()
        self._workers = []

    def _launch_backend(self):
        kwargs = dict(
            stay_alive=True,
            max_age=self.task_expiry_age,
            source=self.source_url,
            sink=self.result_url,
        )
        work_thread = threading.Thread(target=run, kwargs=kwargs)
        work_thread.start()
        self.status.add_task(str(uuid.uuid4()), work_thread)

    def _stop_task(self, task):
        task.join()