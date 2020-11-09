import os
import shutil

from util.constants import DIR_REPO


class BaseArtifactStatsCollector(object):
    def __init__(self, is_debug=False):
        self.is_debug = is_debug
        self.metrics = {}
        self.build_path = os.path.join(DIR_REPO, "build")

    def setup(self):
        """ Run any setup for the test such as compiling, starting the DB, etc. """
        os.mkdir(self.build_path)

    def run_collector(self):
        """ This function is where the logic for the collector belongs """
        pass

    def teardown(self):
        """ Return all test state to the same as it was at the start of the collector """
        shutil.rmtree(self.build_path)

    def get_metrics(self):
        """ Return all the metrics that were stored for this collector """
        return self.metrics
