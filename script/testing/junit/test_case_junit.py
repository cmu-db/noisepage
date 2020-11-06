#!/usr/bin/python3
import os
from junit import constants
from util.test_case import TestCase


class TestCaseJUnit(TestCase):
    """
    Class to run JUnit tests
    """

    def __init__(self, args, test_command=constants.JUNIT_TEST_CMD_ALL):
        TestCase.__init__(self, args)
        self.test_command = test_command
        self.set_junit_attributes(args)

    def set_junit_attributes(self, args):
        # junit specific attribute
        self.test_command_cwd = constants.JUNIT_TEST_DIR
        self.test_error_msg = constants.JUNIT_TEST_ERROR_MSG
        self.test_output_file = self.args.get("test_output_file")
        if not self.test_output_file:
            self.test_output_file = constants.JUNIT_OUTPUT_FILE
