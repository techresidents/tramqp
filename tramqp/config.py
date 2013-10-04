
class ConnectionConfig(object):
    """AMQP Connection Config"""

    def __init__(self,
            host,
            port=5672,
            transport='socket',
            vhost='/',
            heartbeat=5):
        """ConnectionConfig constructor

        Args:
            host: amqp hostname
            port: amqp port
            transport: haigha transport
            vhost: amqp vhost
            heartbeat: amqp heartbeat interval in seconds
        """
        self.host = host
        self.port = port
        self.transport = transport
        self.vhost = vhost
        self.heartbeat = heartbeat

class ExchangeConfig(object):
    """AMQP Exchange Config"""

    def __init__(self,
            exchange,
            type,
            passive=False,
            durable=False,
            auto_delete=True,
            internal=False,
            arguments=None):
        """ExchangeConfig constructor

        Args:
            exchange: amqp exchange name
            type: amqp exchange type
            passive: amqp passive flag
            durable: amqp durable flag
            auto_delete: amqp auto_delete flag
            internal: amqp internal flag
            arguments: amqp exchange arguments
        """
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
        """QueueConfig constructor

        Args:
            queue: amqp queue name
              if not provided a random name will be generated
              by the amqp server.
            passive: amqp passive flag
            durable: amqp durable flag
            auto_delete: amqp auto_delete flag
            internal: amqp internal flag
            arguments: amqp queue arguments
        """
        self.queue = queue
        self.passive = passive
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments or {}
