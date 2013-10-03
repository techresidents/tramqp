
class AMQPException(Exception):
    pass

class QueueEmpty(AMQPException):
    pass

class QueueFull(AMQPException):
    pass

class QueueStopped(AMQPException):
    pass
