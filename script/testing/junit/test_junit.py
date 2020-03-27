#!/usr/bin/python3
from junit import constants
from util.test_server import TestServer


class TestJUnit(TestServer):
    """ Class to run JUnit tests """
    def __init__(self, args):
        TestServer.__init__(self, args)
        self.test_command = constants.JUNIT_TEST_COMMAND
        self.test_command_cwd = constants.JUNIT_TEST_DIR
        self.test_error_msg = constants.JUNIT_TEST_ERROR_MSG
        self.test_output_file = self.args.get("test_output_file")
        if not self.test_output_file:
            self.test_output_file = constants.JUNIT_OUTPUT_FILE
