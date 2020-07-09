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
        self.test_error_msg = ""

        # whether the test should run on a fresh or used database
        self.test_fresh_db_on_start = True

        # after the test case finish, whether the database instance should stop or not
        self.test_stop_db_on_finish = True

    def run_pre_test(self):
        pass

    def run_post_test(self):
        pass
