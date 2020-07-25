#!/usr/bin/python3
import os
from junit import constants
from util.test_server_v2 import TestServerV2


class TestJUnit(TestServerV2):
    """
    Class to run JUnit tests
    """

    def __init__(self, args):
        TestServerV2.__init__(self, args)

        self.query_mode = args.get("query_mode", "simple")
        self.set_env_vars(self.query_mode)

    def set_env_vars(self, query_mode):
        # set env var for QUERY_MODE
        os.environ["TERRIER_QUERY_MODE"] = query_mode

        # set env var for PREPARE_THRESHOLD if the QUERY_MODE is 'extended'
        if query_mode == "extended":
            prepare_threshold = self.args.get(
                "prepare_threshold", constants.DEFAULT_PREPARE_THRESHOLD)
            os.environ["TERRIER_PREPARE_THRESHOLD"] = str(prepare_threshold)
