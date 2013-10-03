import functools
import logging
import threading
import time
import Queue

from trpycore.thread.util import join

from tramqp.connection import RabbitConnection
from tramqp.exceptions import QueueEmpty, QueueFull, QueueStopped
from tramqp.message import Message

class MessageContextManager(object):
    def __init__(self,
            ack_queue,
            amqp_msg,
            message_class=Message):
        self.ack_queue = ack_queue
        self.amqp_msg = amqp_msg
        self.message_class = message_class
    
    def __enter__(self):
        """Context manager enter method."""
        message = self.message_class.parse(str(self.amqp_msg.body))
        return message
    
    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit method.

        Args:
            exc_type: exception type if an exception was raised
                within the scope of the context manager, None otherwise.
            exc_value: exception value if an exception was raised
                within the scope of the context manager, None otherwise.
            traceback: exception traceback if an exception was raised
                within the scope of the context manager, None otherwise.
        """
        if exc_type is not None:
            self.nack()
        else:
            self.ack()
    
    def ack(self):
        self.ack_queue.put((True, self.amqp_msg))

    def nack(self):
        self.ack_queue.put((False, self.amqp_msg))
    
class ConsumeQueue(object):
    Empty = QueueEmpty
    Full = QueueFull
    Stopped = QueueStopped

    def __init__(self,
            connection_config,
            exchange_config,
            queue_config,
            routing_keys=None,
            prefetch_count=1,
            requeue_nack=False,
            queue=None,
            retry_queue=None,
            queue_class=Queue.Queue,
            message_class=Message,
            debug=False):
        
        self.connection_config = connection_config
        self.exchange_config = exchange_config
        self.queue_config = queue_config
        self.routing_keys = routing_keys or ['']
        self.prefetch_count = prefetch_count
        self.requeue_nack = requeue_nack
        self.queue = queue or queue_class()
        self.retry_queue = retry_queue
        self.message_class = message_class
        self.debug = debug
        self.log = logging.getLogger(self.__class__.__name__)

        #amqp connection
        self.connection = None
        #amqp channel
        self.channel = None
        #queue containing (ack, amqp_msg) tuples where ack is boolean
        #indicating that message has been processed and needs
        #to be ack'd (True) or nack'd (False)
        self.ack_queue = queue_class()
        #running flag indicated queue has been started and not stopped
        self.running = False
        #flag indicated if connected to amqp server
        self.connected = False
        #flag indicated if amqp channel/exchange/queue is ready for consumption
        self.ready = False

        #amqp connection thread which must only read from socket
        self.connection_thread = None
        #amqp queue thread which must only write to socket
        self.queue_thread = None
    
    def start(self):
        if not self.running:
            if self.connection_thread:
                self.connection_thread.join()
            if self.queue_thread:
                self.queue_thread.join()

            self.running = True
            self.connection_thread = threading.Thread(target=self.run_connection)
            self.queue_thread = threading.Thread(target=self.run_queue)
            self.connection_thread.start()
            self.queue_thread.start()

    def run_connection(self):
        while self.running:
            try:
                if self.connection is None:
                    self.connection = self._create_connection(
                            self.connection_config)
                else:
                    self.connection.read_frames()
            except Exception as e:
                self.log.exception(e)
                time.sleep(1)

        if self.connection:
            self.connection.close()

        self.running = False


    def run_queue(self):
        while self.running:
            try:
                if not self.ready:
                    time.sleep(1)
                else:
                    timeout = self.connection_config.heartbeat
                    ack, amqp_msg = self.ack_queue.get(True, timeout)
                    if ack:
                        self._ack(self.channel, amqp_msg)
                    else:
                        self._nack(self.channel, amqp_msg, self.requeue_nack)
            except Queue.Empty:
                pass
            except Exception as e:
                self.log.exception(e)
                time.sleep(1)

        if self.connection:
            self.connection.close()

        self.running = False

    
    def get(self, block=True, timeout=None):
        if not self.running:
            raise self.Stopped()

        try:
            amqp_msg = self.queue.get(block, timeout)
            message_context =  MessageContextManager(
                    ack_queue=self.ack_queue,
                    amqp_msg=amqp_msg,
                    message_class=self.message_class)
            return message_context
        except Queue.Empty:
            raise self.Empty()

    def stop(self):
        if self.running:
            self.running = False

    def join(self, timeout=None):
        if self.connection_thread and self.queue_thread:
            join([self.connection_thread, self.queue_thread], timeout)

    def _create_connection(self, connection_config):
        connection = RabbitConnection(
                host=connection_config.host,
                port=connection_config.port,
                transport=connection_config.transport,
                vhost=connection_config.vhost,
                heartbeat=connection_config.heartbeat,
                open_cb=self._on_connect,
                close_cb=self._on_disconnect,
                logger=self.log,
                debug=self.debug)
        return connection

    def _create_channel(self, connection, exchange_config):
        channel = connection.channel()
        channel.basic.qos(prefetch_count=self.prefetch_count)
        self._declare_exchange(channel, exchange_config)
        return channel

    def _declare_exchange(self, channel, exchange_config):
        callback = functools.partial(self._on_exchange_declared, channel)

        channel.exchange.declare(
                exchange=exchange_config.exchange,
                type=exchange_config.type,
                passive=exchange_config.passive,
                durable=exchange_config.durable,
                auto_delete=exchange_config.auto_delete,
                internal=exchange_config.internal,
                arguments=exchange_config.arguments,
                cb=callback)

    def _declare_queue(self, channel, queue_config):
        callback = functools.partial(self._on_queue_declared, channel)
        channel.queue.declare(
                queue=queue_config.queue,
                passive=queue_config.passive,
                durable=queue_config.durable,
                exclusive=queue_config.exclusive,
                auto_delete=queue_config.auto_delete,
                arguments=queue_config.arguments,
                cb=callback)

    def _bind_queue(self, channel, exchange_config, queue_config):
        
        channel.basic.consume(queue=queue_config.queue, consumer=self._on_msg, no_ack=False)

        for routing_key in self.routing_keys:
            callback = functools.partial(self._on_queue_bound, channel,
                    queue_config.queue, routing_key)

            channel.queue.bind(
                    queue=queue_config.queue,
                    exchange=exchange_config.exchange,
                    routing_key=routing_key,
                    cb=callback)

    def _ack(self, channel, amqp_msg):
        tag = amqp_msg.delivery_info["delivery_tag"]
        channel.basic.ack(delivery_tag=tag)

    def _nack(self, channel, amqp_msg, requeue=False):
        tag = amqp_msg.delivery_info["delivery_tag"]
        channel.basic.nack(delivery_tag=tag, requeue=requeue)
        if not requeue:
            self._retry(channel, amqp_msg)
    
    def _retry(self, channel, amqp_msg):
        if self.retry_queue:
            message = self.message_class.parse(str(amqp_msg.body))
            self.retry_queue.put(message)

    def _on_connect(self, connection):
        self.connected = True
        self.channel = self._create_channel(connection, self.exchange_config)

    def _on_disconnect(self, connection):
        if self.queue_config.queue.startswith('amq.'):
            self.queue_config.queue = ''

        self.ready = False
        self.connected = False
        self.connection = None
        self.channel = None

    def _on_exchange_declared(self, channel):
        self._declare_queue(channel, self.queue_config)

    def _on_queue_declared(self, channel, queue, msg_count, consume_count):
        self.queue_config.queue = queue
        self._bind_queue(channel, self.exchange_config, self.queue_config)
    
    def _on_queue_bound(self, channel, queue, routing_key):
        self.ready = True

    def _on_msg(self, amqp_msg):
        self.queue.put(amqp_msg)
