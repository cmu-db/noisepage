from ..util.common import expect_command
from ..util.test_server import TestServer
from . import constants


class TestOLTPBench(TestServer):
    """
    TestOLTPBench will build OLTPBench in the pre-suite.
    All other behavior is identical to TestServer.
    """

    def __init__(self, args):
        super().__init__(args, quiet=False)

    def run_pre_suite(self):
        super().run_pre_suite()
        if not self.is_dry_run:
            self._clean_oltpbench()
            self._download_oltpbench()
            self._build_oltpbench()

    def _clean_oltpbench(self):
        """
        Remove the OLTPBench directory from a hardcoded default location.
        Raises an exception if anything goes wrong.
        """
        expect_command(constants.OLTPBENCH_GIT_CLEAN_COMMAND)

    def _download_oltpbench(self):
        """
        Clone the OLTPBench directory to a hardcoded default location.
        Raises an exception if anything goes wrong.
        """
        expect_command(constants.OLTPBENCH_GIT_CLONE_COMMAND)

    def _build_oltpbench(self):
        """
        Build OLTPBench in its hardcoded default location.
        Raises an exception if anything goes wrong.
        Assumes that _download_oltpbench() has already been run.
        """
        for command in constants.OLTPBENCH_ANT_COMMANDS:
            expect_command(command)
