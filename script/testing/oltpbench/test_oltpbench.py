#!/usr/bin/python3
import os
import sys
import subprocess
import json
import traceback
import shutil
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
            self.args.get("loader_threads",
                          constants.OLTP_DEFAULT_LOADER_THREADS))
        self.time = int(
            self.args.get("client_time", constants.OLTP_DEFAULT_TIME))
        self.weights = str(self.args.get("weights"))
        self.transaction_isolation = str(
            self.args.get("transaction_isolation",
                          constants.OLTP_DEFAULT_TRANSACTION_ISOLATION))
        self.query_mode = self.args.get("query_mode")

        # oltpbench xml file paths
        xml_file = "{}_config.xml".format(self.benchmark)
        self.xml_config = os.path.join(constants.OLTP_DIR_CONFIG, xml_file)
        self.xml_template = os.path.join(constants.OLTP_DIR_CONFIG,
                                         "sample_{}".format(xml_file))

        # for different testing results files, please use the unified filename
        # when there are new attributes, please update the suffix
        self.filename_suffix = "{BENCHMARK}_w{WEIGHTS}_s{SCALEFACTOR}_t{TERMINALS}_l{LOADERTHREAD}".format(
            BENCHMARK=self.benchmark,
            WEIGHTS=self.weights.replace(",", "_"),
            SCALEFACTOR=str(self.scalefactor),
            TERMINALS=self.terminals,
            LOADERTHREAD=self.loader_threads)

        # base directory for the result files, default is in the 'oltp_result' folder under the current directory
        self.test_result_base_dir = self.args.get("test_result_dir")
        if not self.test_result_base_dir:
            self.test_result_base_dir = os.path.join(
                os.getcwd(), "oltp_result")

        # after the script finishes, this director will include the generated files: expconfig, summary, etc.
        self.test_result_dir = os.path.join(
            self.test_result_base_dir, self.filename_suffix)

        # oltpbench test results
        self.test_output_file = self.args.get("test_output_file")
        if not self.test_output_file:
            self.test_output_file = os.path.join(
                self.test_result_dir, "oltpbench.log")

        # oltpbench historgrams results - json format
        self.test_histograms_json_file = self.args.get("test_json_histograms")
        if not self.test_histograms_json_file:
            self.test_histograms_json_file = "oltp_histograms_" + self.filename_suffix + ".json"
        self.test_histogram_path = os.path.join(
            constants.OLTP_GIT_LOCAL_PATH, self.test_histograms_json_file)

        # oltpbench test command
        self.test_command = "{BIN} -b {BENCHMARK} -c {XML} -d {RESULTS} {FLAGS} -json-histograms {HISTOGRAMS}".format(
            BIN=constants.OLTP_DEFAULT_BIN,
            BENCHMARK=self.benchmark,
            RESULTS=self.test_result_dir,
            XML=self.xml_config,
            FLAGS=constants.OLTP_DEFAULT_COMMAND_FLAGS,
            HISTOGRAMS=self.test_histogram_path)
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
            return ErrorCode.ERROR

    def create_result_dir(self):
        if not os.path.exists(self.test_result_dir):
            os.makedirs(self.test_result_dir)

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

    def get_db_url(self):
        """ format the DB URL for the JDBC connection """
        # format the url base
        db_url_base = "jdbc:postgresql://{}:{}/terrier".format(
            self.db_host, self.db_port)
        # format the url params
        db_url_params = ""
        if self.query_mode:
            db_url_params += "?preferQueryMode={}".format(self.query_mode)
        # aggregate the url
        db_url = "{BASE}{PARAMS}".format(BASE=db_url_base,
                                         PARAMS=db_url_params)
        return db_url

    def config_xml_file(self):
        xml = ElementTree.parse(self.xml_template)
        root = xml.getroot()
        root.find("dbtype").text = constants.OLTP_DEFAULT_DBTYPE
        root.find("driver").text = constants.OLTP_DEFAULT_DRIVER
        root.find("DBUrl").text = self.get_db_url()
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
        if not os.path.exists(self.test_histogram_path):
            print("=" * 50)
            print("Directory Contents: {}".format(
                os.path.dirname(self.test_histogram_path)))
            print("\n".join(
                os.listdir(os.path.dirname(self.test_histogram_path))))
            print("=" * 50)
            msg = "Unable to find OLTP-Bench result file '{}'".format(
                self.test_histogram_path)
            raise RuntimeError(msg)

        with open(self.test_histogram_path) as oltp_result_file:
            test_result = json.load(oltp_result_file)
        unexpected_result = test_result.get("unexpected", {}).get("HISTOGRAM")
        if unexpected_result and unexpected_result.keys():
            for test in unexpected_result.keys():
                if (unexpected_result[test] != 0):
                    print(str(unexpected_result))
                    sys.exit(ErrorCode.ERROR)
        else:
            raise RuntimeError(str(unexpected_result))

