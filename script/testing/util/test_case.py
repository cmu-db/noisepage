from .common import *
from .mem_metrics import MemoryMetrics


class TestCase:
    """
    TestCase is generally part of a test suite.

    Attributes
    ----------
    args : dict
        The arguments to the test.
    db_host : str
        The hostname that the DBMS lives on.
    db_port : int
        The port that the DBMS is listening on.
    db_restart : bool
        True if the DBMS should restart before the test begins.
    mem_metrics : MemoryMetrics
        The memory metrics for the test.
    test_command : str
        The command that the test should run.
    test_command_cwd : str
        The working directory to execute the test command in.
    test_output_file : str
        The path to which output should be written.
    """

    def __init__(self, args):
        # Strip arguments which were not set.
        self.args = {k: v for k, v in args.items() if v}

        self.db_host = self.args.get("db_host", constants.DEFAULT_DB_HOST)
        self.db_port = self.args.get("db_port", constants.DEFAULT_DB_PORT)
        self.test_output_file = self.args.get("test_output_file", constants.DEFAULT_TEST_OUTPUT_FILE)

        self.test_command = ""
        self.test_command_cwd = None
        self.db_restart = True
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
