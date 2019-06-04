#!/usr/bin/env python

import argparse
import os.path
import socket
import subprocess
import sys
import time
import traceback

class RunJunit:
    """ Class to run Junit tests """

    def __init__(self, args):
        """ Locations and misc. variable initialization """
        self.args = args

        # server output
        self.db_server_output_file = "/tmp/db_server_log.txt"
        # Ant Junit execution output
        self.junit_output_file = "/tmp/junit_log.txt"

        self._set_server_path()
        self.db_server_process = None

        # db server location
        self.db_server_host = "localhost"
        self.db_server_port = 15721
        return

    def _set_server_path(self):
        """ location of db server, relative to this script """

        # builds on Jenkins are in build/<build_type>
        # but CLion creates cmake-build-<build_type>/<build_type>
        # determine what we have and set the server path accordingly
        bin_name = "terrier"
        build_type = args['build_type']
        path_list = ["../../../build/{}".format(build_type),
                     "../../../cmake-build-{}/{}".format(build_type, build_type)]
        for dir in path_list:
            path = os.path.join(dir, bin_name)
            if os.path.exists(path):
                self.db_server_path = path
                return

        msg = "No Db_Server binary found in {}".format(path_list)
        raise RuntimeError(msg)
        return

    def _check_db_server_binary(self):
        """ Check that a Db_Server binary is available """
        if not os.path.exists(self.db_server_path):
            abs_path = os.path.abspath(self.db_server_path)
            msg = "No Db_Server binary found at {}".format(abs_path)
            raise RuntimeError(msg)
        return

    def _run_db_server(self):
        """ Start the Db_Server server """
        self.db_server_output_fd = open(self.db_server_output_file, "w+")
        self.db_server_process = subprocess.Popen(self.db_server_path,
                                                  stdout=self.db_server_output_fd,
                                                  stderr=self.db_server_output_fd)
        self._wait_for_db_server()
        return

    def _wait_for_db_server(self):
        """ Wait for the db_server server to come up.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # max wait of 10s in 0.1s increments
        for i in range(100):
            try:
                s.connect((self.db_server_host, self.db_server_port))
                s.close()
                print ("connected to server in {} seconds".format(i*0.1))
                return
            except:
                time.sleep(0.1)
                continue
        return

    def _stop_db_server(self):
        """ Stop the Db_Server server and print it's log file """
        # get exit code, if any
        self.db_server_process.poll()
        if self.db_server_process.returncode is not None:
            # Db_Server terminated already
            self.db_server_output_fd.close()
            self._print_output(self.db_server_output_file)
            msg = "Db_Server terminated with return code {}".format(
                self.db_server_process.returncode)
            raise RuntimeError(msg)

        # still (correctly) running, terminate it
        self.db_server_process.terminate()
        return

    def _print_output(self, filename):
        """ Print out contents of a file """
        fd = open(filename)
        lines = fd.readlines()
        for line in lines:
            print (line.strip())
        fd.close()
        return

    def _run_junit(self):
        """ Run the JUnit tests, via ant """
        self.junit_output_fd = open(self.junit_output_file, "w+")
        # use ant's junit runner, until we deprecate Ubuntu 14.04.
        # (i.e. ant test)
        # At that time switch to "ant testconsole" which invokes JUnitConsole
        # runner. It requires Java 1.8 or later, but has much cleaner
        # human readable output
        ret_val = subprocess.call(["ant testconsole"],
                                  stdout=self.junit_output_fd,
                                  stderr=self.junit_output_fd,
                                  shell=True)
        self.junit_output_fd.close()
        return ret_val

    def run(self):
        """ Orchestrate the overall JUnit test execution """
        self._check_db_server_binary()
        self._run_db_server()
        ret_val = self._run_junit()
        self._print_output(self.junit_output_file)

        self._stop_db_server()
        if ret_val:
            # print the db_server log file, only if we had a failure
            self._print_output(self.db_server_output_file)
        return ret_val

if __name__ == "__main__":

    aparser = argparse.ArgumentParser(description="junit runner")

    aparser.add_argument('--build_type',
                         default="debug",
                         choices=['debug', 'release'],
                         help="Build type (default: %(default)s")

    args = vars(aparser.parse_args())

    # Make it so that we can invoke the script from any directory.
    # Actual execution has to be from the junit directory, so first
    # determine the absolute directory path to this script
    prog_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    # cd to the junit directory
    os.chdir(prog_dir)

    try:
        junit = RunJunit(args)
        exit_code = junit.run()
    except:
        print ("Exception trying to run junit tests")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1

    sys.exit(exit_code)

