import threading
from asyncio import DefaultEventLoopPolicy


class _ThreadLocalLoopPolicy(DefaultEventLoopPolicy):

    def get_event_loop(self):
        """Get the event loop.

        This may be None or an instance of EventLoop.
        """
        if (self._local._loop is None and
            not self._local._set_called):
            self.set_event_loop(self.new_event_loop())
        if self._local._loop is None:
            raise RuntimeError('There is no current event loop in thread %r.'
                               % threading.current_thread().name)
        return self._local._loop