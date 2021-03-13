from super_user.lib.task_runner.context.status import RunContextTaskStatus


class ThreadRunContextTaskStatus(RunContextTaskStatus):
    def __init__(self, **kwargs):
        super(ThreadRunContextTaskStatus, self).__init__(**kwargs)

    def check_alive(self, task):
        return task.is_alive()