import uuid
from super_user.lib.task_runner.context import RunContext
from status import ECSRunContextTaskStatus


class ECSRunContext(RunContext):
    DEFAULT_MEM_RESERVATION_GB = 3
    DEFAULT_TIMEOUT_SECONDS = 3600

    def __init__(self, env_id, event_sink, source_url, result_url, ecs_executor, ecs_client=None, task_role_arn="", execution_role_arn="", secrets_arn="", ecs_status=None, num_desired_tasks=2, task_expiry_age=2400, refresh_frequency=30):
        super(ECSRunContext, self).__init__(env_id, event_sink, source_url, result_url, task_expiry_age)
        self._executor = ecs_executor
        self._ecs_client = ecs_client if ecs_client else ecs_executor.ecs_client
        self.status = ECSRunContextTaskStatus(self.tags, task_expiry_age=task_expiry_age, refresh_frequency=refresh_frequency)
        if ecs_status:
            self.status = ecs_status
        else:
            self.status = ECSRunContextTaskStatus()
        self.event_sink = event_sink
        self.task_role_arn = task_role_arn
        self.execution_role_arn = execution_role_arn
        self.secrets_arn = secrets_arn

    @property
    def tags(self):
        return {"env_id": self.env_id, "kind": "webscript_env"}

    def _launch_backend(self):
        worker_id = uuid.uuid1()
        tags = {"worker_id": worker_id}
        tags.update(self.tags)
        aws_tags = [dict(key=key, value=value) for key, value in tags.items()]
        return worker_id, self._executor.run_task(
            self._start_command(),
            self.DEFAULT_MEM_RESERVATION_GB,
            timeout_seconds=self.DEFAULT_TIMEOUT_SECONDS,
            task_role_arn=self.task_role_arn,
            execution_role_arn=self.execution_role_arn,
            secrets_arn=self.secrets_arn,
            tags=tags
        )

    @property
    def needs_refresh(self):
        return self.status.num_available_tasks < self.num_desired_tasks

    @property
    def ready(self):
        return self.status.num_available_tasks > 0 and not self.killed

    def _stop_task(self, task):
        try:
            self._ecs_client.stop_task(cluster=task['clusterArn'], task=task['taskArn'],
                                       reason="Context killed for {}".format(self))
        except Exception as e:
            self.logger.error("Failed to send stop_task to ecs...", e)
