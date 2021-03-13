from marshmallow import Schema, fields, post_load, validates_schema, ValidationError


class RPSchema(Schema):
    @classmethod
    def message_handler(cls):
        def handler(msg):
            return cls().dumps(msg).data
        return handler


class RunResult(object):
    def __init__(self, id=None, output=None, stdout=None, exception=None):
        self.id = str(id)
        self.output = output
        self.exception = exception
        self.stdout = stdout

    def is_success(self):
        return self.exception is None

    def __repr__(self):
        return "RunResult(id: {}, output: {}, exception: {}, stdout: {})".format(self.id, self.output, self.exception, self.stdout)


class RunResultSchema(RPSchema):
    id = fields.Str(required=True)
    output = fields.Str(required=False, default=None, allow_none=True)
    exception = fields.Str(required=False, default=None, allow_none=True)
    stdout = fields.Str(required=False, default=None, allow_none=True)

    @post_load
    def make_run_result(self, data):
        return RunResult(**data)

    @validates_schema
    def validate(self, data):
        if not data['stdout'] and not data['exception']:
            raise ValidationError("Must include either an exception or a stdout")


class RunCommandSchema(RPSchema):
    env_id = fields.Str(required=True)
    run_id = fields.Str(required=True)
    run_by = fields.Str(required=True)
    timeout = fields.Int(required=False, default=0)

    @post_load
    def make_run_command(self, data):
        return RunCommand(**data)


class RunCommand(object):
    def __init__(self, env_id=None, run_id=None, run_by=None, timeout=0):
        self.run_id = run_id
        self.run_by = run_by
        self.env_id = env_id
        self.timeout = timeout


default_encoders = {RunCommand: RunCommandSchema.message_handler(), RunResult: RunResultSchema.message_handler()}
