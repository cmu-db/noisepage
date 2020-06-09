#!/usr/bin/python3
import argparse
import os
import socket
import subprocess
import sys
import time
import traceback
import errno

from util import constants
from util.common import *

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

        # test execution output
        self.test_output_file = self.args.get(
            "test_output_file", constants.DEFAULT_TEST_OUTPUT_FILE)

        # test execution command
        self.test_command = ""
        self.test_command_cwd = None
        self.test_error_msg = ""

        return

    def run_pre_test(self):
        pass

    def run_post_test(self):
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
            path = os.path.join(dir, bin_name)
            if os.path.exists(path):
                self.db_path = path
                return

        msg = "No DB binary found in {}".format(path_list)
        raise RuntimeError(msg)
        return

    def check_db_binary(self):
        """ Check that a Db binary is available """
        if not os.path.exists(self.db_path):
            abs_path = os.path.abspath(self.db_path)
            msg = "No DB binary found at {}".format(abs_path)
            raise RuntimeError(msg)
        return

    def run_db(self):
        """ Start the DB server """
        
        # Allow ourselves to try to restart the DBMS multiple times
        for attempt in range(constants.DB_START_ATTEMPTS):
            # Kill any other terrier processes that our listening on our target port
            for other_pid in check_port(self.db_port):
                print("Killing existing server instance listening on port {} [PID={}]".format(self.db_port, other_pid))
                os.kill(other_pid, signal.SIGKILL)
            ## FOR
          
            self.db_output_fd = open(self.db_output_file, "w+")
            self.db_process = subprocess.Popen(self.db_path,
                                               stdout=self.db_output_fd,
                                               stderr=self.db_output_fd)
            try:
                self.wait_for_db()
                break
            except:
                self.stop_db()
                print("+"*100)
                print("DATABASE OUTPUT")
                self.print_output(self.db_output_file)
                if attempt + 1 == constants.DB_START_ATTEMPTS:
                    raise
                traceback.print_exc(file=sys.stdout)
                pass
        ## FOR
        return

    def wait_for_db(self):
        """ Wait for the db server to come up """

        # Check that PID is running
        if not check_pid(self.db_process.pid):
            raise RuntimeError("Unable to find DBMS PID {}".format(self.db_process.pid))

        # Wait a bit before checking if we can connect to give the system time to setup
        time.sleep(constants.DB_START_WAIT)

        # flag to check if the db is running
        is_db_running = False

        # Keep trying to connect to the DBMS until we run out of attempts or we succeeed
        for i in range(constants.DB_CONNECT_ATTEMPTS):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.connect((self.db_host, int(self.db_port)))
                s.close()
                print("Connected to server in {} seconds [PID={}]".format(i * constants.DB_CONNECT_SLEEP, self.db_process.pid))
                is_db_running = True
                break
            except:
                if i > 0 and i % 20 == 0:
                    print("Failed to connect to DB server [Attempt #{}/{}]".format(i, constants.DB_CONNECT_ATTEMPTS))
                    # os.system('ps aux | grep terrier | grep {}'.format(self.db_process.pid))
                    # os.system('lsof -i :15721')
                    traceback.print_exc(file=sys.stdout)
                time.sleep(constants.DB_CONNECT_SLEEP)
                continue

        if not is_db_running:
            msg = "Unable to connect to DBMS [PID={} / {}]"
            status = "RUNNING"
            if not check_pid(self.db_process.pid):
                status = "NOT RUNNING"
            msg = msg.format(self.db_process.pid, status)
            raise RuntimeError(msg)
        return

    def stop_db(self):
        """ Stop the Db server and print it's log file """
        # get exit code, if any
        self.db_process.poll()
        if self.db_process.returncode is not None:
            # Db terminated already
            self.db_output_fd.close()
            self.print_output(self.db_output_file)
            msg = "DB terminated with return code {}".format(
                self.db_process.returncode)
            raise RuntimeError(msg)

        # still (correctly) running, terminate it
        self.db_process.terminate()
        return

    def print_output(self, filename):
        """ Print out contents of a file """
        fd = open(filename)
        lines = fd.readlines()
        for line in lines:
            print(line.strip())
        fd.close()
        return

    def run_test(self):
        """ Run the tests """
        # run the pre test tasks
        self.run_pre_test()

        # run the actual test
        self.test_output_fd = open(self.test_output_file, "w+")
        ret_val, _, _ = run_command(self.test_command,
                                    self.test_error_msg,
                                    stdout=self.test_output_fd,
                                    stderr=self.test_output_fd,
                                    cwd=self.test_command_cwd)
        self.test_output_fd.close()

        # run the post test tasks
        self.run_post_test()
        return ret_val

    def run(self):
        """ Orchestrate the overall test execution """
        self.check_db_binary()
        ret_val = None
        try:
            self.run_db()
            ret_val = self.run_test()
            self.print_output(self.test_output_file)
        except:
            traceback.print_exc(file=sys.stdout)
            ret_val = -1
            pass

        self.stop_db()
        if ret_val is None or ret_val != 0:
            # print the db log file, only if we had a failure
            self.print_output(self.db_output_file)
        return ret_val
    

