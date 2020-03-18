#!/usr/bin/python
import subprocess
from util import constants
from util.test_server import TestServer


class TestOLTPBench(TestServer):
    """ Class to run OLTP Bench tests """
    def __init__(self, args):
        TestServer.__init__(self, args)
        self.test_command = constants.JUNIT_TEST_COMMAND
        self.test_output_file = constants.JUNIT_DB_OUTPUT_FILE

    def run_pre_test(self):
        # install the OLTP
        self.install_oltp()

    def run_post_test(self):
        pass

    def install_oltp(self):
        self.download_oltp()
        self.build_oltp()

    def download_oltp(self):
        p = subprocess.Popen(constants.OLTP_GIT_COMMAND,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != constants.ErrorCode.SUCCESS:
            print("Error: unable to git clone OLTP source code")
            print(stderr)
            os.exit(constants.ErrorCode.ERROR)

    def build_oltp(self):
        for command in constants.OTLP_ANT_COMMANDS:
            p = subprocess.Popen(command,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            if p.returncode != constants.ErrorCode.SUCCESS:
                print(stderr)
                os.exit(constants.ErrorCode.ERROR)
