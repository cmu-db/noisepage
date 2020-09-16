#!/usr/bin/python3
import argparse
import os
import socket
import subprocess
import shlex
import sys
import time
import traceback
import errno

from util import constants
from util.test_case import TestCase
from util.common import *
from util.constants import LOG

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
        self.continue_on_error = self.args.get("continue_on_error", constants.DEFAULT_CONTINUE_ON_ERROR)
        return

    def run_pre_suite(self):
        pass

    def run_post_suite(self):
        pass

    def set_db_path(self):
        """ location of db server, relative to this script """

        # builds on Jenkins are in build/<build_type>
        # but CLion creates cmake-build-<build_type>/<build_type>
        # determine what we have and set the server path accordingly
        bin_name = constants.DEFAULT_DB_BIN
        build_type = self.args.get("build_type", "")
        path_list = [
            os.path.join(constants.DIR_REPO, "build", build_type),
            os.path.join(constants.DIR_REPO,
                         "cmake-build-{}".format(build_type), build_type)
        ]
        for dir in path_list:
            db_bin_path = os.path.join(dir, bin_name)
            if os.path.exists(db_bin_path):
                path = db_bin_path
                server_args = self.args.get("server_args","").strip()
                if server_args:
                    path = db_bin_path + " "+ server_args
                self.db_bin_path = db_bin_path
                self.db_path = path
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
            # Kill any other terrier processes that our listening on our target port
            kill_processes_listening_on_db_port(self.db_port)

            self.db_output_fd, self.db_process = start_db(self.db_path,
                                                          self.db_output_file)
            try:
                self.wait_for_db()
                break
            except:
                self.stop_db()
                LOG.error("+" * 100)
                LOG.error("DATABASE OUTPUT")
                print_output(self.db_output_file)
                if attempt + 1 == constants.DB_START_ATTEMPTS:
                    raise
                traceback.print_exc(file=sys.stdout)
                pass
        # FOR
        return
    
    def wait_for_db(self):
        """ Wait for the db server to come up """

        # Check that PID is running
        check_db_process_exists(self.db_process.pid)

        # Wait a bit before checking if we can connect to give the system time to setup
        time.sleep(constants.DB_START_WAIT)

        # flag to check if the db is running
        is_db_running = False

        attempt_number = 0
        # Keep trying to connect to the DBMS until we run out of attempts or we succeeed
        for i in range(constants.DB_CONNECT_ATTEMPTS):
            attempt_number = i+1
            is_db_running = check_db_running(self.db_host, self.db_port)
            if is_db_running:
                break
            else:
                if attempt_number % 20 == 0:
                    LOG.error("Failed to connect to DB server [Attempt #{}/{}]".
                          format(attempt_number, constants.DB_CONNECT_ATTEMPTS))
                    # os.system('ps aux | grep terrier | grep {}'.format(self.db_process.pid))
                    # os.system('lsof -i :15721')
                    traceback.print_exc(file=sys.stdout)
                time.sleep(constants.DB_CONNECT_SLEEP)

        handle_db_connection_status(is_db_running, attempt_number, db_pid=self.db_process.pid)
        return

    def stop_db(self):
        """ Stop the Db server and print it's log file """
        if not self.db_process:
            return

        # get exit code, if any
        self.db_process.poll()
        if self.db_process.returncode is not None:
            # Db terminated already
            self.db_output_fd.close()
            print_output(self.db_output_file)
            msg = "DB terminated with return code {}".format(
                self.db_process.returncode)
            raise RuntimeError(msg)

        # still (correctly) running, terminate it
        self.db_process.terminate()
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
        if type(test_suite) is not list: test_suite = [ test_suite ]
        ret_val_test_suite = None
        try:
            self.check_db_binary()
            self.run_pre_suite()

            test_suite_ret_vals = self.run_test_suite(test_suite)
            test_suite_result = self.determine_test_suite_result(test_suite_ret_vals)
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
            if test_case.db_restart:
                self.restart_db()
            elif not self.db_process:
                self.run_db()
            
            try:
                test_case_ret_val = self.run_test(test_case)
                print_output(test_case.test_output_file)
                test_suite_ret_vals[test_case] = test_case_ret_val
            except:
                print_output(test_case.test_output_file)
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
            print_output(self.db_output_file)
        if self.continue_on_error:
            return constants.ErrorCode.SUCCESS
        return test_suite_result

def kill_processes_listening_on_db_port(db_port):
    """Kills any processes that are listening on the db_port"""
    for other_pid in check_port(db_port):
        LOG.info("Killing existing server instance listening on port {} [PID={}]"
                    .format(db_port, other_pid))
        os.kill(other_pid, signal.SIGKILL)

def start_db(db_path, db_output_file):
    """
    Starts the DB process based on the DB path and write stdout and sterr
    to the db_output_file. This returns the db output file descriptor and 
    the db_process created by Popen.
    """
    db_output_fd = open(db_output_file,"w+")
    LOG.info("Server start: {PATH}".format(PATH=db_path))
    db_process = subprocess.Popen(shlex.split(db_path),
                                    stdout=db_output_fd,
                                    stderr=db_output_fd)
    return db_output_fd, db_process

def print_output(filename):
    """ Print out contents of a file """
    with open(filename) as file:
        lines = file.readlines()
        for line in lines:
            LOG.info(line.strip())

def check_db_process_exists(db_pid):
    """ Checks to see if the db_pid exists """
    if not check_pid(db_pid):
        raise RuntimeError("Unable to find DBMS PID {}".format(db_pid))
    else:
        LOG.info("DBMS running on PID {}".format(db_pid))

def check_db_running(db_host, db_port):
    """
    Tries to connect to the DBMS. Returns True if it connected 
    and False otherwise
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((db_host, int(db_port)))
        s.close()
        return True
    except:
        return False

def handle_db_connection_status(is_db_running, attempt_number, db_pid):
    """
    Based on whether the DBMS is running and whether the db_pid exists this
    will print the appropriate message or throw an error.
    """
    if not is_db_running:
        LOG.error(
            "Failed to connect to DB server [Attempt #{ATTEMPT}/{TOTAL_ATTEMPTS}]"
            .format(ATTEMPT=attempt_number,
            TOTAL_ATTEMPTS=constants.DB_CONNECT_ATTEMPTS)
        )
        check_db_process_exists(db_pid)
        raise RuntimeError('Unable to connect to DBMS.')
    else:
        LOG.info("Connected to server in {} seconds [PID={}]".format(
                    attempt_number * constants.DB_CONNECT_SLEEP, db_pid))

