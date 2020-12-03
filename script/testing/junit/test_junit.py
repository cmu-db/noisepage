#!/usr/bin/python3
import os
from junit.constants import DEFAULT_PREPARE_THRESHOLD
from util.test_server import TestServer
from util.constants import LOG


class TestJUnit(TestServer):
    """
    Class to run JUnit tests
    """

    def __init__(self, args):
        TestServer.__init__(self, args)

        self.query_mode = args.get("query_mode", "simple")
        self.set_env_vars(self.query_mode, args.get('prepare_threshold', DEFAULT_PREPARE_THRESHOLD))

    def set_env_vars(self, query_mode, prepare_threshold):
        # set env var for QUERY_MODE
        os.environ["NOISEPAGE_QUERY_MODE"] = query_mode

        # set env var for PREPARE_THRESHOLD if the QUERY_MODE is 'extended'
        if query_mode == "extended":
            prepare_threshold_env_var = str(prepare_threshold) if prepare_threshold else str(DEFAULT_PREPARE_THRESHOLD)
            os.environ["NOISEPAGE_PREPARE_THRESHOLD"] = prepare_threshold_env_var
