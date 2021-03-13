import logging
import time
from multiprocessing import Pool

from super_user.lib.task_runner.utils import GracefulKiller


class TaskRunner(object):
    STOP_VALUE = 'STOP'

    def __init__(self, stay_alive, start_time, max_age, event_sink, work_source, task_handlers=dict(), killer=GracefulKiller(), max_concurrency=4, kill_after=None):
        self.stay_alive = stay_alive
        self.end_time = start_time + max_age
        self.event_sink = event_sink
        self.work_source = work_source
        self.max_concurrency = max_concurrency
        self.logger = logging.getLogger(self.__class__.__name__)
        self.num_running = 0
        self._task_handlers = task_handlers
        self.pool = Pool(processes=max_concurrency)
        self.killer = killer
        self._handles=[]
        self.kill_after = kill_after

    def register_handler(self, event_cls, handler):
        self._task_handlers[event_cls] = handler

    def _shut_down(self):
        self.logger.warn("Shutdown called...")
        self.logger.debug("Closing out running process handles...")
        for handle in self._handles:
            handle.wait()
        self.logger.debug("Process handles closed.")
        self.logger.debug("Closing pool...")
        self.pool.close()
        self.pool.terminate()
        self.logger.debug("Joining process pool...")
        self.pool.join()
        self.logger.debug("Shutdown successful.")

    def _get_work_callback(self, work_id):
        def callback(result):
            self.event_sink.send_event(result)
            self.work_source.ack_work(work_id)
        return callback

    def _get_task_handler(self, task):
        handler = self._task_handlers.get(task.__class__)
        if not handler:
            raise KeyError("No task handler defined for class {}".format(task.__class__.__name__))
        return handler

    def _do_work(self):
        work_id, work = self.work_source.get_work()
        if work_id:
            handler = self._get_task_handler(work)
            callback = self._get_work_callback(work_id)
            return self.pool.apply_async(handler, args=[work], callback=callback)

    def start(self):
        num_processed = 0
        while not self.killer.kill_now and time.time() < self.end_time and self.num_running < self.max_concurrency:
            if self.kill_after and self.kill_after == num_processed:
                break
            work_handle = self._do_work()
            if work_handle:
                self._handles.append(work_handle)
            num_processed += 1
        self._shut_down()