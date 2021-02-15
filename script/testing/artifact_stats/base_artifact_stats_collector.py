from abc import ABC, ABCMeta, abstractmethod


class BaseArtifactStatsCollector(ABC):
    """
    This is the base class to use if you want to collect a new artifact metric.
    You can override the setup, run_collector, and teardown methods for your
    specific collector implementation.

    Use the with context-manager to handle the calling of setup and teardown.

    Attributes
    ----------
    is_debug : bool
        This determines whether debug output from the collector will be printed
        in stdout. In many cases this will be the stdout of a spawned process.
    metrics : dict
        The metrics collected during the execution of the collector.
    """
    __metaclass__ = ABCMeta

    def __init__(self, is_debug=False):
        """
        Initialize the artifact stats collector.

        Parameters
        ----------
        is_debug : bool
            True iff the debug output should be printed to stdout.
        """
        self.is_debug = is_debug
        self.metrics = {}

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.teardown()

    def setup(self):
        """
        This function is guaranteed to be called ONCE before run_collector().
        Override this to perform any setup steps required.
        """
        pass

    @abstractmethod
    def run_collector(self):
        """
        This function must be overridden by all collectors.
        It should directly mutate self.metrics and perform any steps necessary
        to capture the artifact stats for the collector.

        Returns
        -------
        exit_code : int
            0 on success, and anything else on failure.
        """
        pass

    def teardown(self):
        """
        This function is guaranteed to always be called ONCE at the end.
        The end might mean after setup(), after run_collector(),
        or after an exception happened.
        Override this to perform any teardown steps required.
        """
        pass
