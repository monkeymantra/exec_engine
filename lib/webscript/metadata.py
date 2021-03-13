import json
import boto3


class WebscriptEnvironmentMetadata(dict):
    def __init__(self, *args, **kwargs):
        super(WebscriptEnvironmentMetadata, self).__init__(kwargs)

    @staticmethod
    def from_ssm(path):
        client = boto3.client('ssm')
        result = json.loads(client.get_parameter(Name=path)['Parameter']['Value'])
        return WebscriptEnvironmentMetadata(**result)