from ..util.common import expect_command
from ..util.test_server import TestServer
from . import constants


class TestSqlancer(TestServer):
    """
    TestSqlancer will build Sqlancer in the pre-suite.
    All other behavior is identical to TestServer.
    """

    def __init__(self, args):
        super().__init__(args, quiet=False)

    def run_pre_suite(self):
        super().run_pre_suite()
        if not self.is_dry_run:
            self._clean_sqlancer()
            self._download_sqlancer()
            self._build_sqlancer()

    def _clean_sqlancer(self):
        """
        Remove the Sqlancer directory from a hardcoded default location.
        Raises an exception if anything goes wrong.
        """
        expect_command(constants.SQLANCER_GIT_CLEAN_COMMAND)

    def _download_sqlancer(self):
        """
        Clone the Sqlancer directory to a hardcoded default location.
        Raises an exception if anything goes wrong.
        """
        expect_command(constants.SQLANCER_GIT_CLONE_COMMAND)

    def _build_sqlancer(self):
        """
        Build Sqlancer in its hardcoded default location.
        Raises an exception if anything goes wrong.
        Assumes that _download_sqlancer() has already been run.
        """
        expect_command(constants.SQLANCER_BUILD_COMMAND, cwd=constants.SQLANCER_GIT_LOCAL_PATH)
