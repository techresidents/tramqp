from haigha.connections import RabbitConnection as HaighaRabbitConnection

class RabbitConnection(HaighaRabbitConnection):
    def _callback_open(self):
        if self._open_cb:
            self._open_cb(self)

    def _callback_close(self):
        if self._close_cb:
            self._close_cb(self)
