from enum import Enum
from super_user.lib.task_runner.context.ecs import ECSRunContext

RunContextStatus = Enum('RunContextStatus', " ".join(['WORKING', 'LAUNCHING', 'IDLE', 'KILLED', 'STOPPING', 'UNKNOWN']))


class TaskContextManager(object):
    def __init__(self):
        self._contexts = dict()
        pass

    @property
    def contexts(self):
        return self._contexts

    def get_context(self, env_id, timeout=None, context_cls=None):
        context = self.contexts.get(env_id)
        if context and context.ready:
            return context
        else:
            context_cls

    def stop_all(self, timeout=None):
        pass

    def _register_context(self, context):
        pass

    def _create_context(self, env_id, context_class=ECSRunContext, **config):
        self._register_context(context_class(env_id, **config))


