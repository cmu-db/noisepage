#!/usr/bin/env python3
import os
import sys
import time
import traceback
from typing import List
from util import constants
from util.db_server import NoisePageServer
from util.test_case import TestCase
from util.constants import LOG, ErrorCode
from util.periodic_task import PeriodicTask
from util.common import (run_command, print_file, print_pipe, update_mem_info)


class TestServer:
    """ Class to run general tests """

    def __init__(self, args):
        """ Locations and misc. variable initialization """
        # clean up the command line args
        args = {k: v for k, v in args.items() if v}

        # server output
        db_output_file = args.get("db_output_file", constants.DEFAULT_DB_OUTPUT_FILE)
        db_host = args.get("db_host", constants.DEFAULT_DB_HOST)
        db_port = args.get("db_port", constants.DEFAULT_DB_PORT)
        build_type = args.get("build_type", "")
        server_args = args.get("server_args", {})

        self.db_instance = NoisePageServer(db_host, db_port, build_type, server_args, db_output_file)

        # whether the server should stop the whole test if one of test cases failed
        self.continue_on_error = args.get("continue_on_error", constants.DEFAULT_CONTINUE_ON_ERROR)

        # memory info collection
        self.collect_mem_info = args.get("collect_mem_info", False)

        # incremental metrics
        self.incremental_metric_freq = args.get("incremental_metric_freq", constants.INCREMENTAL_METRIC_FREQ)
        return

    def run_pre_suite(self):
        pass

    def run_post_suite(self):
        pass

    def run_test(self, test_case: TestCase):
        """ Run the tests """
        if not test_case.test_command or not test_case.test_command_cwd:
            msg = "test command should be provided"
            raise RuntimeError(msg)

        # run the pre test tasks
        test_case.run_pre_test()

        # start a thread to collect the memory info if needed
        if self.collect_mem_info:
            # spawn a thread to collect memory info
            self.collect_mem_thread = PeriodicTask(
                self.incremental_metric_freq, update_mem_info,
                self.db_instance.db_process.pid, self.incremental_metric_freq,
                test_case.mem_metrics.mem_info_dict)
            # collect the initial memory info
            update_mem_info(self.db_instance.db_process.pid, self.incremental_metric_freq,
                            test_case.mem_metrics.mem_info_dict)

        # run the actual test
        with open(test_case.test_output_file, "w+") as test_output_fd:
            ret_val, _, _ = run_command(test_case.test_command,
                                        test_case.test_error_msg,
                                        stdout=test_output_fd,
                                        stderr=test_output_fd,
                                        cwd=test_case.test_command_cwd)

        # stop the thread to collect the memory info if started
        if self.collect_mem_info:
            self.collect_mem_thread.stop()

        # run the post test tasks
        test_case.run_post_test()
        self.db_instance.delete_wal()

        return ret_val

    def run(self, test_suite):
        """ Orchestrate the overall test execution """
        if not isinstance(test_suite, List):
            test_suite = [test_suite]

        try:
            self.run_pre_suite()

            test_suite_ret_vals = self.run_test_suite(test_suite)
            test_suite_result = self.determine_test_suite_result(
                test_suite_ret_vals)
        except:
            traceback.print_exc(file=sys.stdout)
            test_suite_result = constants.ErrorCode.ERROR
        finally:
            # after the test suite finish, stop the database instance
            self.db_instance.stop_db()
        return self.handle_test_suite_result(test_suite_result)

    def run_test_suite(self, test_suite):
        """ Execute all the tests in the test suite """
        test_suite_ret_vals = {}
        for test_case in test_suite:
            try:
                # catch the exception from run_db(), stop_db(), and restart_db()
                # in case the db is unable to start/stop/restart
                if test_case.db_restart:
                    self.db_instance.restart_db()
                elif not self.db_instance.db_process:
                    self.db_instance.run_db()
            except:
                traceback.print_exc(file=sys.stdout)
                test_suite_ret_vals[test_case] = constants.ErrorCode.ERROR
                # early termination in case of db is unable to start/stop/restart
                break

            try:
                test_case_ret_val = self.run_test(test_case)
                print_file(test_case.test_output_file)
                test_suite_ret_vals[test_case] = test_case_ret_val
            except:
                print_file(test_case.test_output_file)
                if not self.continue_on_error:
                    raise
                else:
                    traceback.print_exc(file=sys.stdout)
                    test_suite_ret_vals[test_case] = constants.ErrorCode.ERROR
        return test_suite_ret_vals

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
