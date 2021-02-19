import sys
import traceback

from . import constants
from .common import print_file, run_command, update_mem_info
from .constants import LOG
from .db_server import NoisePageServer
from .periodic_task import PeriodicTask
from .test_case import TestCase


class TestServer:
    """
    TestServer is a wrapper around NoisePageServer that can run test suites.

    Attributes
    ----------
    collect_mem_info : bool
        True if memory information should be collected.
    continue_on_error : bool
        True if the failure of a test case should stop an entire test suite.
    db_instance : NoisePageServer
        The DBMS that is being tested.
    incremental_metric_freq : int
        The frequency at which incremental metrics should be collected.
    is_dry_run : bool
        True if the TestServer will only perform dry-runs, meaning that
        testing code will be printed instead of being executed.
    """

    def __init__(self, args, quiet=False):
        """
        Create a new TestServer instance.

        Parameters
        ----------
        args : dict
            The arguments to the server.
        quiet : bool
            True if successful tests should avoid printing verbose output.
        """

        # Strip arguments which were not set.
        args = {k: v for k, v in args.items() if v is not None}

        self._quiet = quiet
        self.is_dry_run = args.get("dry_run", False)
        self.continue_on_error = args.get("continue_on_error", constants.DEFAULT_CONTINUE_ON_ERROR)
        self.collect_mem_info = args.get("collect_mem_info", False)
        self.incremental_metric_freq = args.get("incremental_metric_freq", constants.INCREMENTAL_METRIC_FREQ)

        db_host = args.get("db_host", constants.DEFAULT_DB_HOST)
        db_port = args.get("db_port", constants.DEFAULT_DB_PORT)
        build_type = args.get("build_type", "")
        server_args = args.get("server_args", {})
        db_output_file = args.get("db_output_file", constants.DEFAULT_DB_OUTPUT_FILE)
        self.db_instance = NoisePageServer(db_host, db_port, build_type, server_args, db_output_file)

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

        collect_mem_thread = None
        if self.collect_mem_info:
            # Initialize memory information dict.
            update_mem_info(self.db_instance.db_process.pid,
                            self.incremental_metric_freq,
                            test_case.mem_metrics.mem_info_dict)
            # Start a thread to collect memory information periodically.
            collect_mem_thread = PeriodicTask(
                self.incremental_metric_freq, update_mem_info,
                self.db_instance.db_process.pid, self.incremental_metric_freq,
                test_case.mem_metrics.mem_info_dict)
            collect_mem_thread.start()

        ret_val = constants.ErrorCode.ERROR
        try:
            LOG.info(f"Logging output (overwrite) to: {test_case.test_output_file}")
            with open(test_case.test_output_file, "w") as test_output_fd:
                ret_val, _, _ = run_command(test_case.test_command,
                                            stdout=test_output_fd,
                                            stderr=test_output_fd,
                                            cwd=test_case.test_command_cwd)
        finally:
            if self.collect_mem_info and collect_mem_thread is not None:
                collect_mem_thread.stop()

            test_case.run_post_test()

        return ret_val

    def run(self, test_suite):
        """
        Orchestrate the overall execution of the specified test suite.

        Parameters
        ----------
        test_suite : [TestCase]
            A list of test cases to be run.

        Returns
        -------
        suite_result : constants.ErrorCode
            SUCCESS if all the tests succeeded. ERROR otherwise.
        """
        result = constants.ErrorCode.ERROR
        try:
            self.run_pre_suite()
            exit_codes = self.run_test_suite(test_suite)
            has_error = any([x is None or x != constants.ErrorCode.SUCCESS
                             for x in exit_codes.values()])
            result = constants.ErrorCode.ERROR if has_error else constants.ErrorCode.SUCCESS
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception:
            traceback.print_exc(file=sys.stdout)
        finally:
            self.run_post_suite()

        if result is None or result != constants.ErrorCode.SUCCESS:
            LOG.error("The test suite failed")
        return result

    def run_test_suite(self, test_suite):
        """
        Run the provided test suite.

        Parameters
        ----------
        test_suite : [TestCase]
            A list of test cases to be run.

        Returns
        -------
        exit_codes : dict
            A dictionary mapping test cases to their exit codes.
        """
        exit_codes = {}
        dbms_started = self.db_instance.run_db(self.is_dry_run)
        first_run = True
        for test_case in test_suite:
            try:
                if test_case.db_restart and self.db_instance.db_process and not first_run:
                    self.db_instance.stop_db(self.is_dry_run)
                    dbms_started = self.db_instance.run_db(self.is_dry_run)
                first_run = False
                if not self.is_dry_run:
                    try:
                        exit_code = self.run_test(test_case)
                        if self._quiet and exit_code == 0:
                            LOG.info("Quiet and all tests passed (return code 0), skipping printing.")
                        else:
                            print_file(test_case.test_output_file)
                        exit_codes[test_case] = exit_code
                    except KeyboardInterrupt:
                        raise KeyboardInterrupt
                    except Exception:
                        print_file(test_case.test_output_file)
                        if not self.continue_on_error:
                            raise
                        else:
                            traceback.print_exc(file=sys.stdout)
                            exit_codes[test_case] = constants.ErrorCode.ERROR
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except Exception:
                traceback.print_exc(file=sys.stdout)
                exit_codes[test_case] = constants.ErrorCode.ERROR
                # Terminate early in case the DBMS was unable to start.
                break

        # The DBMS is not restarted between tests as you may want the DBMS to
        # persist create/load data.
        if dbms_started:
            self.db_instance.stop_db(self.is_dry_run)
            self.db_instance.delete_wal()

        return exit_codes
