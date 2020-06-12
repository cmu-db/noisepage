#!/usr/bin/python3
import os
import sys
import subprocess
import json
import traceback
from util.constants import ErrorCode
from util.common import run_command
from util.test_server import TestServer
from xml.etree import ElementTree
from oltpbench import constants


class TestOLTPBench(TestServer):
    """ Class to run OLTP Bench tests """
    def __init__(self, args):
        TestServer.__init__(self, args)

        # oltpbench specific attributes
        self.benchmark = str(self.args.get("benchmark"))
        self.scalefactor = float(
            self.args.get("scale_factor", constants.OLTP_DEFAULT_SCALEFACTOR))
        self.terminals = int(
            self.args.get("terminals", constants.OLTP_DEFAULT_TERMINALS))
        self.loader_threads = int(
            self.args.get("loader_threads", constants.OLTP_DEFAULT_LOADER_THREADS))
        self.time = int(
            self.args.get("client_time", constants.OLTP_DEFAULT_TIME))
        self.weights = str(self.args.get("weights"))
        self.transaction_isolation = str(
            self.args.get("transaction_isolation",
                          constants.OLTP_DEFAULT_TRANSACTION_ISOLATION))

        # oltpbench xml file paths
        xml_file = "{}_config.xml".format(self.benchmark)
        self.xml_config = os.path.join(constants.OLTP_DIR_CONFIG, xml_file)
        self.xml_template = os.path.join(constants.OLTP_DIR_CONFIG,
                                         "sample_{}".format(xml_file))

        # oltpbench test results
        self.test_output_file = self.args.get("test_output_file")
        if not self.test_output_file:
            self.test_output_file = "oltp_output_{BENCHMARK}_{WEIGHTS}_{SCALEFACTOR}".format(
                BENCHMARK=self.benchmark,
                WEIGHTS=self.weights.replace(",", "_"),
                SCALEFACTOR=str(self.scalefactor))
            # TODO: ask Andy to remove the relative path from the oltpbench for execution and result logging
            # self.test_output_file = os.path.join(
            #     constants.OLTP_DIR_TEST_RESULT, self.result_path)

        # oltpbench json format test results
        self.test_output_json_file = self.args.get("test_output_json_file")
        if not self.test_output_json_file:
            self.test_output_json_file = "outputfile_{WEIGHTS}_{SCALEFACTOR}.json".format(
                WEIGHTS=self.weights.replace(",", "_"),
                SCALEFACTOR=str(self.scalefactor))

        # oltpbench json result file paths
        self.oltpbench_result_path = os.path.join(
            constants.OLTP_GIT_LOCAL_PATH, self.test_output_json_file)

        # oltpbench test command
        self.test_command = "{BIN} -b {BENCHMARK} -c {XML} {FLAGS} -json-histograms {RESULTS}".format(
            BIN=constants.OLTP_DEFAULT_BIN,
            BENCHMARK=self.benchmark,
            XML=self.xml_config,
            FLAGS=constants.OLTP_DEFAULT_COMMAND_FLAGS,
            RESULTS=self.oltpbench_result_path)
        self.test_command_cwd = constants.OLTP_GIT_LOCAL_PATH
        self.test_error_msg = constants.OLTP_TEST_ERROR_MSG

    def run_pre_test(self):
        # install the OLTP
        self.install_oltp()
        self.config_xml_file()
        self.create_result_dir()

    def run_post_test(self):
        # validate the OLTP result
        try:
            self.validate_result()
        except:
            traceback.print_exc(file=sys.stdout)
            pass

    def create_result_dir(self):
        if not os.path.exists(constants.OLTP_DIR_TEST_RESULT):
            os.mkdir(constants.OLTP_DIR_TEST_RESULT)

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

    def config_xml_file(self):
        xml = ElementTree.parse(self.xml_template)
        root = xml.getroot()
        root.find("dbtype").text = constants.OLTP_DEFAULT_DBTYPE
        root.find("driver").text = constants.OLTP_DEFAULT_DRIVER
        root.find(
            "DBUrl"
        ).text = "jdbc:postgresql://{}:{}/terrier?preferQueryMode=simple".format(
            self.db_host, self.db_port)
        root.find("username").text = constants.OLTP_DEFAULT_USERNAME
        root.find("password").text = constants.OLTP_DEFAULT_PASSWORD
        root.find("isolation").text = str(self.transaction_isolation)
        root.find("scalefactor").text = str(self.scalefactor)
        root.find("terminals").text = str(self.terminals)
        for work in root.find("works").findall("work"):
            work.find("time").text = str(self.time)
            work.find("rate").text = str(constants.OLTP_DEFAULT_RATE)
            work.find("weights").text = str(self.weights)

        # Loader Threads
        # The sample config files usually do not contain this parameter, so we may
        # have to insert it into the XML tree
        if root.find("loaderThreads") is None:
            loaderThreads = ElementTree.Element("loaderThreads")
            loaderThreads.text = str(self.loader_threads)
            root.insert(1, loaderThreads)
        else:
            root.find("loaderThreads").text = str(self.loader_threads)
        
        xml.write(self.xml_config)

    def validate_result(self):
        """read the results file"""

        # Make sure the file exists before we try to open it.
        # If it's not there, we'll dump out the contents of the directory to make it
        # easier to determine whether or not we are crazy when running Jenkins.
        if not os.path.exists(self.oltpbench_result_path):
            print("=" * 50)
            print("Directory Contents: {}".format(
                os.path.dirname(self.oltpbench_result_path)))
            print("\n".join(
                os.listdir(os.path.dirname(self.oltpbench_result_path))))
            print("=" * 50)
            msg = "Unable to find OLTP-Bench result file '{}'".format(
                self.oltpbench_result_path)
            raise RuntimeError(msg)

        with open(self.oltpbench_result_path) as oltp_result_file:
            test_result = json.load(oltp_result_file)
        unexpected_result = test_result.get("unexpected", {}).get("HISTOGRAM")
        if unexpected_result and unexpected_result.keys():
            for test in unexpected_result.keys():
                if (unexpected_result[test] != 0):
                    print(str(unexpected_result))
                    sys.exit(constants.ErrorCode.ERROR)
        else:
            print(str(unexpected_result))
            sys.exit(constants.ErrorCode.ERROR)
