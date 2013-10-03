class ConnectionConfig(object):
    def __init__(self,
            host,
            port=5672,
            transport='socket',
            vhost='/',
            heartbeat=5):
        self.host = host
        self.port = port
        self.transport = transport
        self.vhost = vhost
        self.heartbeat = heartbeat

class ExchangeConfig(object):
    def __init__(self,
            exchange,
            type,
            passive=False,
            durable=False,
            auto_delete=True,
            internal=False,
            arguments=None):
        self.exchange = exchange
        self.type = type
        self.passive = passive
        self.durable = durable
        self.auto_delete = auto_delete
        self.internal = internal
        self.arguments = arguments or {}

class QueueConfig(object):
    def __init__(self,
            queue='',
            passive=False,
            durable=False,
            exclusive=False,
            auto_delete=True,
            arguments=None):
        self.queue = queue
        self.passive = passive
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments or {}
