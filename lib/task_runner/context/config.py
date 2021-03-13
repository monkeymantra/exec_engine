class RunContextConfig(object):
    REQUIRED = ['env']
    def __init__(self, **kwargs):
        for attr in self.REQUIRED:
            if attr not in kwargs:
                raise TypeError("Required attribute {} missing".format(attr))
        self.kwargs = kwargs

    @property
    def config(self):
        return self.kwargs

    def __getattr__(self, item):
        return self.config.get(item, None)


class ProcessRunContextConfig(RunContextConfig):
    REQUIRED = ['env', 'owner']


class ECSRunContextConfig(RunContextConfig):
    REQUIRED = ['env', 'task_role_arn', 'execution_role_arn', 'secrets_arn', 'sqs_queue_name', 'events_topic']
