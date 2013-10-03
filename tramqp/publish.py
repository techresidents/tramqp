import functools
import logging
import threading
import time
import Queue

from trpycore.thread.result import AsyncResult
from trpycore.thread.util import join

from haigha.message import Message as HaighaMessage

from tramqp.connection import RabbitConnection
from tramqp.exceptions import QueueEmpty, QueueFull, QueueStopped
from tramqp.message import Message

class PublishItem(object):
    def __init__(self, message, priority, routing_key):
        self.message = message
        self.priority = priority
        self.routing_key = routing_key
        self.result = AsyncResult()

class PublishQueue(object):
    #Convenience exception classes
    Empty = QueueEmpty
    Full = QueueFull
    Stopped = QueueStopped
    
    #Stop item to put to queue to stop processing
    STOP_ITEM = object()

    def __init__(self,
            sender,
            connection_config,
            exchange_config,
            routing_key='',
            queue=None,
            queue_class=Queue.PriorityQueue,
            message_class=Message,
            debug=False):

        self.sender = sender
        self.connection_config = connection_config
        self.exchange_config = exchange_config
        self.routing_key = routing_key
        self.queue = queue or queue_class()
        self.message_class = message_class
        self.debug = debug
        self.log = logging.getLogger(self.__class__.__name__)
        
        #amqp connection
        self.connection = None
        #amqp channel
        self.channel = None
        #amqp confirms extension sequence number
        self.seq = 0
        #map from amqp confirms sequence no. to (priority, item) tuple
        self.unack = {}
        #running flag indicated queue has been started and not stopped
        self.running = False
        #flag indicated if connected to amqp server
        self.connected = False
        #flag indicated if amqp channel/exchange is ready for publishing
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
        while self._keep_running():
            try:
                if self.connection is None:
                    self.connection = self._create_connection(self.connection_config)
                else:
                    self.connection.read_frames()
            except Exception as e:
                self.log.exception(e)
                time.sleep(1)

        if self.connection:
            self.connection.close()

        self.running = False

    def run_queue(self):
        while self._keep_running():
            try:
                if not self.ready:
                    time.sleep(1)
                else:
                    timeout = self.connection_config.heartbeat
                    priority, item = self.queue.get(True, timeout)

                    if item is self.STOP_ITEM:
                        self._drain()
                    else:
                        self.seq += 1
                        self.unack[self.seq] = (priority, item)
                        self._publish(item.message, item.routing_key)
            except Queue.Empty:
                pass
            except Exception as e:
                self.log.exception(e)
                time.sleep(1)
        
        #Closing the connection here is not needed if
        #connection_config.heartbeat is non-zero (which it should be)
        #but adding it allows the run_connection thread to exit
        #more quickly since connection.read_frames() will return
        #as soon connection.close() is called. Otherwise the
        #run_connection thread would have to wait up to heartbeat
        #seconds for a new heartbeat message to wake it up and
        #allow it to detect exit conditions.
        if self.connection:
            self.connection.close()

        self.running = False

    def put(self, msg, block=True, timeout=None, routing_key=None):
        if not self.running:
            raise self.Stopped()

        item = self._build_item(msg, routing_key)

        try:
            self.queue.put((item.priority, item), block, timeout)
            return item.result
        except Queue.Full:
            raise self.Full()

    def stop(self):
        self.running = False
        self.queue.put((0, self.STOP_ITEM))

    def join(self, timeout=None):
        if self.connection_thread and self.queue_thread:
            join([self.connection_thread, self.queue_thread], timeout)

    def _create_connection(self, config):
        connection = RabbitConnection(
                host=config.host,
                port=config.port,
                transport=config.transport,
                vhost=config.vhost,
                heartbeat=config.heartbeat,
                open_cb=self._on_connect,
                close_cb=self._on_disconnect,
                logger=self.log,
                debug=self.debug)
        return connection

    def _create_channel(self, connection, config):
        channel = connection.channel()
        self._declare_exchange(channel, config)
        channel.confirm.select()
        channel.basic.set_ack_listener(self._on_publish_ack)
        channel.basic.set_nack_listener(self._on_publish_nack)
        return channel

    def _declare_exchange(self, channel, config):
        callback = functools.partial(self._on_exchange_declared, channel)

        channel.exchange.declare(
                exchange=config.exchange,
                type=config.type,
                passive=config.passive,
                durable=config.durable,
                auto_delete=config.auto_delete,
                internal=config.internal,
                arguments=config.arguments,
                cb=callback)

    def _build_item(self, msg, routing_key):
        now = time.time()
        priority = now

        if isinstance(msg, tuple):
            priority, msg = msg
        if not isinstance(msg, self.message_class):
            msg = self.message_class(msg)
        if msg.header.sender is None:
            msg.header.sender = self.sender
        if msg.header.routing_key is None:
            if routing_key is not None:
                msg.header.routing_key = routing_key
            else:
                msg.header.routing_key = self.routing_key
        if msg.header.timestamp is None:
            msg.header.timestamp = now

        return PublishItem(msg, priority, msg.header.routing_key)

    def _publish(self, msg, routing_key):
        self.channel.basic.publish(
                msg=HaighaMessage(str(msg)),
                exchange=self.exchange_config.exchange,
                routing_key=routing_key,
                mandatory=False,
                immediate=False)

    def _requeue_unack(self):
        unack = dict(self.unack)
        for item in unack.values():
            self.queue.put(item)
        self.unack = {}
    
    def _on_connect(self, connection):
        self.seq = 0
        self.connected = True
        self._requeue_unack()
        self.channel = self._create_channel(connection, self.exchange_config)

    def _on_disconnect(self, connection):
        self.ready = False
        self.connected = False
        self.connection = None
        self.channel = None
        self._requeue_unack()

    def _on_exchange_declared(self, channel):
        self.ready = True

    def _on_publish_ack(self, seq):
        priority, item = self.unack[seq]
        del self.unack[seq]
        item.result.set()

    def _on_publish_nack(self, seq):
        self.queue.put(self.unack[seq])
        priority, item = self.unack[seq]
        del self.unack[seq]
    
    def _drain(self):
        while True:
            try:
                self.queue.get(False)
            except Queue.Empty:
                break

    def _keep_running(self):
        return self.running or not self.queue.empty() or self.unack
