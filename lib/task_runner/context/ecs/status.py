import time

from enum import Enum

from super_user.lib.task_runner.context.status import RunContextTaskStatus

import datetime
from dateutil.tz import tzutc

epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=tzutc())

def unix_time_seconds(dt):
    return (dt - epoch).total_seconds()


class ECSRunContextTaskStatus(RunContextTaskStatus):
    def __init__(self, ecs_client=None, resource_tagging_client=None, tags=dict(), refresh_frequency=30, task_expiry_age=2400):
        super(ECSRunContextTaskStatus, self).__init__(refresh_frequency=refresh_frequency, task_expiry_age=task_expiry_age)
        self.tags = tags
        self.last_updated = 0
        self.refresh_frequency = refresh_frequency
        self.task_retiry_age = task_expiry_age
        self.resource_tagging_client = resource_tagging_client
        self.ecs_client = ecs_client
        self._task_status = None

    def _find_tasks(self):
        task_tag_query_result = self.resource_tagging_client.get_resources(
            TagFilters=[
                {'Key': key, 'Values': [value]} for key, value in self.tags],
            ResourceTypeFilters=[
                'ecs:task',
            ])
        task_arns = [task['ResourceARN'] for task in task_tag_query_result['ResourceTagMappingList']]
        return task_arns

    def _get_workers(self):
        task_arns = self._find_tasks()
        if not task_arns:
            workers = []
        else:
            cluster = task_arns[0].split("/")[1]
            tasks_result = self.ecs_client.describe_tasks(cluster=cluster, tasks=task_arns)
            if not tasks_result.get('failures'):
                workers =  [task for task in tasks_result['tasks']]
            else:
                workers = []
        return workers

    def _update_task_status(self):
        tasks = self._find_tasks()
        if tasks:
            cluster = tasks[0].split("/")[1]
            tasks_result = self.ecs_client.describe_tasks(cluster=cluster, tasks=tasks)
            if not tasks_result.get('failures'):
                workers = [task for task in tasks_result['tasks']]
                for worker in workers:
                    self.add_task(worker['taskArn'], worker, unix_time_seconds(worker['createdAt']))
        self.last_updated = time.time()

    def check_alive(self, task):
        status = ECSStatus[task['lastStatus']]
        return status.is_active()


class ECSStatus(str, Enum):
    PROVISIONING = "PROVISIONING"
    PENDING = "PENDING"
    ACTIVATING = "ACTIVATING"
    RUNNING = "RUNNING"
    DEACTIVATING = "DEACTIVATING"
    STOPPING = "STOPPING"
    DEPROVISIONING = "DEPROVISIONING"
    STOPPED = "STOPPED"

    def is_active(self):
        return self.value in {self.PROVISIONING, self.PENDING, self.ACTIVATING, self.RUNNING}