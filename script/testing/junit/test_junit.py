import os

from ..util.test_server import TestServer
from .constants import DEFAULT_PREPARE_THRESHOLD


class TestJUnit(TestServer):
    """
    Run JUnit tests, which for some reason means setting environment variables.
    TODO(WAN): I don't understand why we are using environment variables.
    """

    def __init__(self, args):
        """
        Parameters
        ----------
        args : dict
            A dictionary that may contain the following keys:
                query_mode : "simple" or "extended"
                prepare_threshold : an int that represents
        """
        TestServer.__init__(self, args)
        self.query_mode = args.get("query_mode", "simple")
        self.prepare_threshold = args.get("prepare_threshold", DEFAULT_PREPARE_THRESHOLD)

        os.environ["NOISEPAGE_QUERY_MODE"] = self.query_mode
        # The prepare threshold is only relevant if the query mode is extended.
        if self.query_mode == "extended":
            os.environ["NOISEPAGE_PREPARE_THRESHOLD"] = str(self.prepare_threshold)
