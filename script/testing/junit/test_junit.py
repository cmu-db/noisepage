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
        self.test_command = constants.JUNIT_TEST_COMMAND
        self.test_command_cwd = constants.JUNIT_TEST_DIR
        self.test_error_msg = constants.JUNIT_TEST_ERROR_MSG
        self.test_output_file = self.args.get("test_output_file")
        if not self.test_output_file:
            self.test_output_file = constants.JUNIT_OUTPUT_FILE

    def run_pre_test(self):
        self.set_env_vars()

    def set_env_vars(self):
        # set env var for QUERY_MODE
        query_mode = self.args.get("query_mode", "simple")
        os.environ["TERRIER_QUERY_MODE"] = query_mode

        # set env var for PREPARE_THRESHOLD if the QUERY_MODE is 'extended'
        if query_mode == "extended":
            prepare_threshold = self.args.get(
                "prepare_threshold", constants.DEFAULT_PREPARE_THRESHOLD)
            os.environ["TERRIER_PREPARE_THRESHOLD"] = str(prepare_threshold)
