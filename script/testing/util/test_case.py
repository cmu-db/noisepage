from .common import *
from .mem_metrics import MemoryMetrics


class TestCase:
    """Class of a test case, could be part of a test suite"""

    def __init__(self, args):
        # Strip
        self.args = {k: v for k, v in args.items() if v}

        self.db_host = self.args.get("db_host", constants.DEFAULT_DB_HOST)
        self.db_port = self.args.get("db_port", constants.DEFAULT_DB_PORT)

        self.test_output_file = self.args.get("test_output_file", constants.DEFAULT_TEST_OUTPUT_FILE)

        self.test_command = ""
        self.test_command_cwd = None
        self.test_error_msg = "Default error message."

        # Whether the DB should restart before the test begin
        self.db_restart = True

        # memory metrics
        self.mem_metrics = MemoryMetrics()

    def run_pre_test(self):
        """
        Code that must always be run before a test.
        """
        pass

    def run_post_test(self):
        """
        Code that must always be run after a test.
        """
        pass
