#!/usr/bin/python3
import os
import socket
import subprocess
import sys
import time
import traceback
import shlex
from typing import List
from util import constants
from util.test_case import TestCase
from util.common import run_command, print_file, run_check_pids, run_kill_server, print_pipe
from util.constants import LOG, ErrorCode


class TestServer:
    """ Class to run general tests """
    def __init__(self, args):
        """ Locations and misc. variable initialization """
        # clean up the command line args
        self.args = {k: v for k, v in args.items() if v}

        # server output
        self.db_output_file = self.args.get("db_output_file",
                                            constants.DEFAULT_DB_OUTPUT_FILE)

        # set the DB path
        self.set_db_path()
        self.db_process = None

        # db server location
        self.db_host = self.args.get("db_host", constants.DEFAULT_DB_HOST)
        self.db_port = self.args.get("db_port", constants.DEFAULT_DB_PORT)

        # whether the server should stop the whole test if one of test cases failed
        self.continue_on_error = self.args.get(
            "continue_on_error", constants.DEFAULT_CONTINUE_ON_ERROR)

        return

    def run_pre_suite(self):
        pass

    def run_post_suite(self):
        pass

    def set_db_path(self):
        """ location of db server, relative to this script """

        # builds on Jenkins are in build/bin
        # but CLion creates cmake-build-<build_type>/bin
        # determine what we have and set the server path accordingly
        bin_name = constants.DEFAULT_DB_BIN
        build_type = self.args.get("build_type", "")
        path_list = [
            os.path.join(constants.DIR_REPO, "build", "bin"),
            os.path.join(constants.DIR_REPO,
                         "cmake-build-{}".format(build_type), "bin")
        ]
        for dir_name in path_list:
            db_bin_path = os.path.join(dir_name, bin_name)
            if os.path.exists(db_bin_path):
                full_path = db_bin_path
                server_args = self.args.get("server_args", "").strip()
                if server_args:
                    full_path = db_bin_path + " " + server_args
                self.db_bin_path = db_bin_path
                self.db_path = full_path
                return

        msg = "No DB binary found in {}".format(path_list)
        raise RuntimeError(msg)

    def check_db_binary(self):
        """ Check that a Db binary is available """
        if not os.path.exists(self.db_bin_path):
            abs_path = os.path.abspath(self.db_bin_path)
            msg = "No DB binary found at {}".format(abs_path)
            raise RuntimeError(msg)
        return

    def run_db(self):
        """ Start the DB server """

        # Allow ourselves to try to restart the DBMS multiple times
        for attempt in range(constants.DB_START_ATTEMPTS):
            # Kill any other noisepage processes that our listening on our target port

            # early terminate the run_db if kill_server.py encounter any exceptions
            run_kill_server(self.db_port)

            # use memory buffer to hold db logs
            self.db_process = subprocess.Popen(shlex.split(self.db_path),
                                               stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE)
            LOG.info("Server start: {PATH} [PID={PID}]".format(
                PATH=self.db_path, PID=self.db_process.pid))

            if not run_check_pids(self.db_process.pid):
                # The DB process does not exist, try starting it again
                continue

            check_line = "[info] Listening on Unix domain socket with port {PORT} [PID={PID}]".format(
                PORT=self.db_port, PID=self.db_process.pid)
            while True:
                db_log_line_raw = self.db_process.stdout.readline()
                if not db_log_line_raw:
                    break
                db_log_line_str = db_log_line_raw.decode("utf-8").rstrip("\n")
                LOG.info(db_log_line_str)
                if db_log_line_str.endswith(check_line):
                    LOG.info("DB process is verified as running")
                    return

        msg = "Failed to start DB after {} attempts".format(
            constants.DB_START_ATTEMPTS)
        raise RuntimeError(msg)

    def stop_db(self):
        """ Stop the Db server and print it's log file """
        if not self.db_process:
            return

        # get exit code if any
        self.db_process.poll()
        if self.db_process.returncode is not None:
            # Db terminated already
            msg = "DB terminated with return code {}".format(
                self.db_process.returncode)
            LOG.info("DB exited with return code {}".format(
                self.db_process.returncode))
            self.print_db_logs()
            raise RuntimeError(msg)
        else:
            # still (correctly) running, terminate it
            self.db_process.terminate()
            LOG.info("DB stops normally")
            self.print_db_logs()
        self.db_process = None
        return

    def restart_db(self):
        """ Restart the DB """
        self.stop_db()
        self.run_db()

    def run_test(self, test_case: TestCase):
        """ Run the tests """
        if not test_case.test_command or not test_case.test_command_cwd:
            msg = "test command should be provided"
            raise RuntimeError(msg)

        # run the pre test tasks
        test_case.run_pre_test()

        # run the actual test
        with open(test_case.test_output_file, "w+") as test_output_fd:
            ret_val, _, _ = run_command(test_case.test_command,
                                        test_case.test_error_msg,
                                        stdout=test_output_fd,
                                        stderr=test_output_fd,
                                        cwd=test_case.test_command_cwd)

        # run the post test tasks
        test_case.run_post_test()

        return ret_val

    def run(self, test_suite):
        """ Orchestrate the overall test execution """
        if not isinstance(test_suite, List):
            test_suite = [test_suite]

        ret_val_test_suite = None
        try:
            self.check_db_binary()
            self.run_pre_suite()

            test_suite_ret_vals = self.run_test_suite(test_suite)
            test_suite_result = self.determine_test_suite_result(
                test_suite_ret_vals)
        except:
            traceback.print_exc(file=sys.stdout)
            test_suite_result = constants.ErrorCode.ERROR
        finally:
            # after the test suite finish, stop the database instance
            self.stop_db()
        return self.handle_test_suite_result(test_suite_result)

    def run_test_suite(self, test_suite):
        """ Execute all the tests in the test suite """
        test_suite_ret_vals = {}
        for test_case in test_suite:
            try:
                # catch the exception from run_db(), stop_db(), and restart_db()
                # in case the db is unable to start/stop/restart
                if test_case.db_restart:
                    self.restart_db()
                elif not self.db_process:
                    self.run_db()
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
        if self.continue_on_error:
            return constants.ErrorCode.SUCCESS
        return test_suite_result

    def print_db_logs(self):
        """	
        Print out the remaining DB logs	
        """
        LOG.info("************ DB Logs Start ************")
        print_pipe(self.db_process)
        LOG.info("************* DB Logs End *************")
