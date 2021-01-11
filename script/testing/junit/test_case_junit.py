from ..util.test_case import TestCase
from . import constants


class TestCaseJUnit(TestCase):
    """
    Class to run JUnit tests.
    """

    def __init__(self, args, test_command=constants.JUNIT_TEST_CMD_ALL):
        TestCase.__init__(self, args)
        self.test_command = test_command
        self.test_command_cwd = constants.JUNIT_TEST_DIR
        self.test_output_file = self.args.get("test_output_file", constants.JUNIT_OUTPUT_FILE)
