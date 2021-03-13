import logging
import time


class RunContextTaskStatus(object):
    def __init__(self, refresh_frequency=30, task_expiry_age=2400):
        self.refresh_frequency = refresh_frequency
        self.task_expiry_age = task_expiry_age
        self.last_updated = 0
        self.start_time = time.time()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.tasks = dict()
        self._task_status = None

    def staleness_in_seconds(self):
        return time.time() - self.last_updated

    def add_task(self, task_id, task, start_time=time.time()):
        self.tasks[task_id] = (task, start_time)

    @property
    def stale(self):
        return self.staleness_in_seconds() > self.refresh_frequency

    def _update_task_status(self):
        pass

    def update_task_status(self):
        self._update_task_status()

    @property
    def task_status(self):
        if self.stale:
            self._update_task_status()
        return self.tasks

    def check_alive(self, task):
        return True

    def check_alive_id(self, task_id):
        check_result = self.check_alive(self.tasks.get(task_id))
        self.logger.info(check_result)
        return check_result

    @property
    def active_workers(self):
        if time.time() - self.last_updated > self.refresh_frequency:
            self.update_task_status()
        for task_id in self.tasks.keys():
            task, start_time = self.tasks.get(task_id)
            try:
                if self.check_alive(task):
                    yield task, start_time
                else:
                    self.logger.info("Task {} not alive".format(task_id))
            except Exception as e:
                    self.logger.error("Couldn't get status of task: {}".format(task_id))

    @property
    def num_available_tasks(self):
        return len(list(self.available_tasks))

    @property
    def available_tasks(self):
        current_time = time.time()
        for task, start_time in self.active_workers:
            task_age = current_time - start_time
            self.logger.info("Task age: {}".format(task_age))
            if current_time - start_time < self.task_expiry_age:
                yield task

