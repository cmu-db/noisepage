from threading import Timer


class PeriodicTask:
    """
    PeriodicTask can schedule simple periodic tasks.
    """

    def __init__(self, interval, function, *args, **kwargs):
        """
        Initialize the PeriodicTask object.
        """
        self._timer = None
        self.function = function
        self.interval = interval
        self.args = args
        self.kwargs = kwargs
        self.is_running = False

    def _run(self):
        """
        Execute the periodic task and reschedule the next Timer thread.
        """
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        """
        Schedule and start the Timer thread.
        """
        assert not self.is_running

        self._timer = Timer(self.interval, self._run)
        self._timer.start()
        self.is_running = True

    def stop(self):
        """
        Cancel and join the running Timer thread.
        """
        self._timer.cancel()
        self._timer.join()
        self.is_running = False
