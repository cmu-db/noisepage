#!/usr/bin/python
from util import constants
from util.test_server import TestServer


class TestJUnit(TestServer):
    """ Class to run JUnit tests """
    def __init__(self, args):
        TestServer.__init__(self, args)
        self.test_command = constants.JUNIT_TEST_COMMAND
