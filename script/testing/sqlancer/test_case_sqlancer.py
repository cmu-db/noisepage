from ..util.test_case import TestCase
from . import constants

class TestCaseSqlancer(TestCase):
    """
    Class to run Sqlancer tests.
    """
    def __init__(self, args, test_command=constants.SQLANCER_TEST_CMD_ALL, test_command_cwd=constants.SQLANCER_TEST_CMD_CWD):
           TestCase.__init__(self, args)
           self.test_command = test_command
           self.test_command_cwd = test_command_cwd
           self.test_output_file = self.args.get("db_output_file", constants.SQLANCER_OUTPUT_FILE)
