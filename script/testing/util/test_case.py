from util import constants
from util.common import *


class TestCase:
    """Class of a test case, could be part of a test suite"""
    def __init__(self, args):
        """ Locations and misc. variable initialization """
        # clean up the command line args
        self.args = {k: v for k, v in args.items() if v}

        # db server location
        self.db_host = self.args.get("db_host", constants.DEFAULT_DB_HOST)
        self.db_port = self.args.get("db_port", constants.DEFAULT_DB_PORT)

        # test execution output
        self.test_output_file = self.args.get(
            "test_output_file", constants.DEFAULT_TEST_OUTPUT_FILE)

        # test execution command
        self.test_command = ""
        self.test_command_cwd = None
        self.test_error_msg = "Unknown Error"

        # whether the DB should restart before the test begin
        self.db_restart = True

        # memory info dict
        self.mem_info_dict = {}

    def run_pre_test(self):
        pass

    def run_post_test(self):
        pass
