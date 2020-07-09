#!/usr/bin/python3
import os
import sys
import subprocess
import json
import traceback
import shutil
from util.constants import ErrorCode
from util.common import run_command
from util.test_server_v2 import TestServerV2
from xml.etree import ElementTree
from oltpbench import constants


class TestOLTPBenchV2(TestServerV2):
    """ Class to run OLTP Bench tests """

    def __init__(self, args):
        TestServerV2.__init__(self, args)

    def run_pre_suite(self):
        self.install_oltp()

    def install_oltp(self):
        self.clean_oltp()
        self.download_oltp()
        self.build_oltp()

    def clean_oltp(self):
        rc, stdout, stderr = run_command(constants.OLTP_GIT_CLEAN_COMMAND,
                                         "Error: unable to clean OLTP repo")
        if rc != ErrorCode.SUCCESS:
            print(stderr)
            sys.exit(rc)

    def download_oltp(self):
        rc, stdout, stderr = run_command(
            constants.OLTP_GIT_COMMAND,
            "Error: unable to git clone OLTP source code")
        if rc != ErrorCode.SUCCESS:
            print(stderr)
            sys.exit(rc)

    def build_oltp(self):
        for command in constants.OLTP_ANT_COMMANDS:
            error_msg = "Error: unable to run \"{}\"".format(command)
            rc, stdout, stderr = run_command(command, error_msg)
            if rc != ErrorCode.SUCCESS:
                print(stderr)
                sys.exit(rc)
