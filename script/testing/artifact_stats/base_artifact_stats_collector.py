import os


class BaseArtifactStatsCollector(object):
    """ This is the base class to use if you want to collect a new artifact 
    metric. You can override the setup, run_collector, and teardown methods 
    for your specific collector implementation.

    Properties:
    `is_debug` - This determines whether debug output from the collector will
                 be printed in stdout. In many cases this will be the stdout 
                 of a spawned process.
    `metrics` - The metrics collected during the execution of the collector.
    """

    def __init__(self, is_debug=False):
        self.is_debug = is_debug
        self.metrics = {}

    def setup(self):
        """ Run any setup for the test such as compiling, starting the DB, etc. """
        pass

    def run_collector(self):
        """ This function is where the logic for the collector belongs """
        pass

    def teardown(self):
        """ Return all test state to the same as it was at the start of the collector """
        pass

    def get_metrics(self):
        """ Return all the metrics that were stored for this collector. Refrain from 
        overriding this method. """
        return self.metrics
