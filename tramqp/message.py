import json

from tramqp.encode import Encoder

class Header(object):
    @classmethod
    def parse(cls, data):
        if isinstance(data, basestring):
            json_data = json.loads(data)
        else:
            json_data = data

        return Header(
                sender=json_data.get("sender"),
                routing_key=json_data.get("routing_key"),
                timestamp=json_data.get("timestamp"))

    def __init__(self, sender=None, routing_key=None,timestamp=None):
        self.sender = sender
        self.timestamp = timestamp
        self.routing_key = routing_key

    def __repr__(self):
        return '%s(%r, %r, %r)' % (self.__class__, self.sender, self.routing_key, self.timestamp)

    def __str__(self):
        return json.dumps(self.to_json(), cls=Encoder)

    def to_json(self):
        return {
            "sender": self.sender,
            "routing_key": self.routing_key,
            "timestamp": self.timestamp
        }

class Message(object):
    @classmethod
    def parse(cls, data):
        if isinstance(data, basestring):
            json_data = json.loads(data)
        else:
            json_data = data

        return Message(
                header=Header.parse(json_data.get("header")),
                body=json_data.get("body"))

    def __init__(self, body, header=None):
        self.header = header or Header()
        self.body = body

    def __repr__(self):
        return '%s(%r, %r)' % (self.__class__, self.header, self.body)

    def __str__(self):
        return json.dumps(self.to_json(), cls=Encoder)

    def to_json(self):
        return {
            "header": self.header,
            "body": self.body
        }
