#!/usr/bin/python3
import os
from junit import constants
from util.test_server import TestServer


class TestJUnit(TestServer):
    """
    Class to run JUnit tests
    """

    def __init__(self, args):
        TestServer.__init__(self, args)

        self.query_mode = args.get("query_mode", "simple")
        self.set_env_vars(self.query_mode)

    def set_env_vars(self, query_mode):
        # set env var for QUERY_MODE
        os.environ["NOISEPAGE_QUERY_MODE"] = query_mode

        # set env var for PREPARE_THRESHOLD if the QUERY_MODE is 'extended'
        if query_mode == "extended":
            prepare_threshold = self.args.get(
                "prepare_threshold", constants.DEFAULT_PREPARE_THRESHOLD)
            os.environ["NOISEPAGE_PREPARE_THRESHOLD"] = str(prepare_threshold)
