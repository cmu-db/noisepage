import sys
import traceback
from typing import List

from . import constants
from .common import print_file, print_pipe, run_command, update_mem_info
from .constants import LOG
from .db_server import NoisePageServer
from .periodic_task import PeriodicTask
from .test_case import TestCase


class TestServer:
    """
    TestServer represents an abstract server or some shit. what the fuck
    """

    def __init__(self, args, quiet=False):
        """

        Parameters
        ----------
        args
        quiet
        """

        # Strip arguments which were not set.
        args = {k: v for k, v in args.items() if v is not None}

        self._quiet = quiet
        self.is_dry_run = args.get("dry_run", False)

        db_host = args.get("db_host", constants.DEFAULT_DB_HOST)
        db_port = args.get("db_port", constants.DEFAULT_DB_PORT)
        build_type = args.get("build_type", "")
        server_args = args.get("server_args", {})
        db_output_file = args.get("db_output_file", constants.DEFAULT_DB_OUTPUT_FILE)
        self.db_instance = NoisePageServer(db_host, db_port, build_type, server_args, db_output_file)

        # whether the server should stop the whole test if one of test cases failed
        self.continue_on_error = args.get("continue_on_error", constants.DEFAULT_CONTINUE_ON_ERROR)

        # memory info collection
        self.collect_mem_info = args.get("collect_mem_info", False)

        # incremental metrics
        self.incremental_metric_freq = args.get("incremental_metric_freq", constants.INCREMENTAL_METRIC_FREQ)

    def run_pre_suite(self):
        """
        Code which will be executed before run_test().
        """
        pass

    def run_post_suite(self):
        """
        Code which will ALWAYS be executed at the end, even if an exception occurs.
        """
        pass

    def run_test(self, test_case: TestCase):
        """
        Run the provided test case.

        Parameters
        ----------
        test_case : TestCase
            The test case that should be run.

        Returns
        -------
        The return value of running the test case.
        """
        if not test_case.test_command:
            raise RuntimeError("Missing test command.")
        if not test_case.test_command_cwd:
            raise RuntimeError("Missing test command working directory.")

        test_case.run_pre_test()

        if self.collect_mem_info:
            # Initialize memory information dict.
            update_mem_info(self.db_instance.db_process.pid,
                            self.incremental_metric_freq,
                            test_case.mem_metrics.mem_info_dict)
            # Start a thread to collect memory information periodically.
            self.collect_mem_thread = PeriodicTask(
                self.incremental_metric_freq, update_mem_info,
                self.db_instance.db_process.pid, self.incremental_metric_freq,
                test_case.mem_metrics.mem_info_dict)
            self.collect_mem_thread.start()

        ret_val = constants.ErrorCode.ERROR
        try:
            with open(test_case.test_output_file, "a+") as test_output_fd:
                ret_val, _, _ = run_command(test_case.test_command,
                                            stdout=test_output_fd,
                                            stderr=test_output_fd,
                                            cwd=test_case.test_command_cwd)
        finally:
            if self.collect_mem_info:
                self.collect_mem_thread.stop()

            test_case.run_post_test()
            self.db_instance.delete_wal()

        return ret_val

    def run(self, test_suite):
        """
        Orchestrate the overall execution of the specified test suite.

        Parameters
        ----------
        test_suite : [TestCase]

        Returns
        -------

        """

        """ Orchestrate the overall test execution """

        # no, fuck you. if your code is bad, you crash. no magic. TODO(WAN)
        if not isinstance(test_suite, List):
            test_suite = [test_suite]

        result = constants.ErrorCode.ERROR
        try:
            self.run_pre_suite()

            exit_codes = self.run_test_suite(test_suite)
            result = self.determine_test_suite_result(exit_codes)
        except:
            traceback.print_exc(file=sys.stdout)
        finally:
            self.run_post_suite()
        return self.handle_test_suite_result(result)

    def run_test_suite(self, test_suite):
        """

        Parameters
        ----------
        test_suite : [TestCase]


        Returns
        -------
        exit_codes : dict
            A dictionary mapping test cases to their exit codes.
        """
        exit_codes = {}
        for test_case in test_suite:
            try:
                self.db_instance.run_db(self.is_dry_run)
                if not self.is_dry_run:
                    try:
                        exit_code = self.run_test(test_case)
                        if self._quiet and exit_code == 0:
                            LOG.info("Quiet and all tests passed (return code 0), skipping printing.")
                        else:
                            print_file(test_case.test_output_file)
                        exit_codes[test_case] = exit_code
                    except:
                        print_file(test_case.test_output_file)
                        if not self.continue_on_error:
                            raise
                        else:
                            traceback.print_exc(file=sys.stdout)
                            exit_codes[test_case] = constants.ErrorCode.ERROR
                self.db_instance.stop_db(self.is_dry_run)
            except:
                traceback.print_exc(file=sys.stdout)
                exit_codes[test_case] = constants.ErrorCode.ERROR
                # early termination in case of db is unable to start/stop/restart
                break

        return exit_codes

    def determine_test_suite_result(self, test_suite_ret_vals):
        """	
        Based on all the test suite resultes this determines whether the test	
        suite was a success or error	
        """
        for test_case, test_result in test_suite_ret_vals.items():
            if test_result is None or test_result != constants.ErrorCode.SUCCESS:
                return constants.ErrorCode.ERROR
        return constants.ErrorCode.SUCCESS

    def handle_test_suite_result(self, test_suite_result):
        """	
        Determine what to do based on the result. If continue_on_error is	
        True then it will mask any errors and return success. Otherwise,	
        it will return the result of the test suite.	
        """
        if test_suite_result is None or test_suite_result != constants.ErrorCode.SUCCESS:
            LOG.error("The test suite failed")
        return test_suite_result

    def print_db_logs(self):
        """	
        Print out the remaining DB logs	
        """
        LOG.info("************ DB Logs Start ************")
        print_pipe(self.db_instance.db_process)
        LOG.info("************* DB Logs End *************")
