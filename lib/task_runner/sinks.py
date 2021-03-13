import logging
import enum
from super_user.lib.task_runner.models import default_encoders

class SinkType(str, enum.Enum):
    sns = "sns"
    socket = "socket"


class EventSink(object):
    def __init__(self, **kwargs):
        self._encoders = kwargs.get('encoders', default_encoders)
        self.logger = logging.getLogger(self.__class__.__name__)

    def _register_encoder(self, cls, encoder):
        self._encoders[cls] = encoder

    def _send_event(self, encoded_event, event_type, **event_meta):
        self.logger.info("Sent event: {}".format(encoded_event))

    def _get_encoder(self, event):
        encoder = self._encoders.get(event.__class__)
        if not encoder:
            raise KeyError("No encoder found for events of type {}".format(event.__class__.__name__))
        return encoder

    def send_event(self, event, **event_meta):
        self.logger.debug("Fetching encoder for {}".format(event))
        encode = self._get_encoder(event)
        encoded = encode(event)
        self.logger.debug("Encoded: {}".format(encoded))
        self._send_event(encoded, event.__class__.__name__, **event_meta)


class SNSEventSink(EventSink):
    def __init__(self, topic_arn, **kwargs):
        client = kwargs.get('client')
        if not client:
            import boto3
        super(SNSEventSink, self).__init__(**kwargs)
        self.topic_arn = topic_arn
        self.client = client if client else boto3.client('sns', 'us-west-2')

    def _send_event(self, encoded_result, event_type, **meta):
        attributes = {
                'event_type': {
                    'DataType': 'String',
                    'StringValue': event_type
                }
            }
        for key, value in meta.items():
            attributes.update({key: {'DataType':'String', 'StringValue': value}})

        message = dict(
            TopicArn=self.topic_arn,
            Message=encoded_result,
            MessageStructure='string',
            MessageAttributes=attributes,
        )
        return self.client.publish(**message)


class SinkFactory(object):
    @staticmethod
    def get_sink(url):
        sink_type, sink_url = url.split("://")
        if sink_type == SinkType.sns:
            return SNSEventSink(sink_url)

