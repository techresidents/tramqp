
class AMQPException(Exception):
    """AMQP Exception base class."""
    pass

class QueueEmpty(AMQPException):
    """QueueEmpty exception"""
    pass

class QueueFull(AMQPException):
    """QueueFull exception"""
    pass

class QueueStopped(AMQPException):
    """QueueStopped exception"""
    pass
