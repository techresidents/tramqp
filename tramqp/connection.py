from haigha.connections import RabbitConnection as HaighaRabbitConnection

class RabbitConnection(HaighaRabbitConnection):
    """RabbitMQ connection

    This class extends HaighaRabbitConnection in order to make
    the connection open and close callbacks more usable by
    adding the connection object as a parameter to the callback.

    This is necessary in the case of a synchronous transport like
    'socket' because the initial AMQP server connection is opened
    synchronously in the HaighaRabbitConnection constructor. This
    means that when the open callback is invoked the connection
    object will not be fully instantiated and no reference to
    the connection object will yet exist.

    Without a referencable connection object, the open callback
    isn't very useful since you cannot create the AMQP channel
    and declare exchanges/queues without a reference to the
    connection object.
    """

    def _callback_open(self):
        if self._open_cb:
            self._open_cb(self)

    def _callback_close(self):
        if self._close_cb:
            self._close_cb(self)
