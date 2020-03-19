#!/usr/bin/python
import os
import sys
import subprocess
from util import constants
from util.test_server import TestServer
from xml.etree import ElementTree


class TestOLTPBench(TestServer):
    """ Class to run OLTP Bench tests """
    def __init__(self, args):
        TestServer.__init__(self, args)
        self.db_host = str(self.args.get(constants.DEFAULT_DB_HOST))
        self.db_port = str(self.args.get(constants.DEFAULT_DB_PORT))

        # oltpbench specific attributes
        self.benchmark = str(self.args.get("benchmark"))
        self.scalefactor = int(
            self.args.get("scalefactor", constants.OLTP_DEFAULT_SCALEFACTOR))
        self.terminals = int(
            self.args.get("terminals", constants.OLTP_DEFAULT_TERMINALS))
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
        self.result_path = "outputfile_{WEIGHTS}_{SCALEFACTOR}".format(
            WEIGHTS=self.weights.replace(",", "_"),
            SCALEFACTOR=str(self.scalefactor))
        self.test_output_file = os.path.join(constants.OLTP_DIR_TEST_RESULT,
                                             self.result_path)

        # oltpbench test command
        self.test_command = "cd {OLTPDIR} && bash {BIN} -b {BENCHMARK} -c {XML} {FLAGS} -o {RESULTS}".format(
            OLTPDIR=constants.OLTP_GIT_LOCAL_PATH,
            BIN=constants.OLTP_DEFAULT_BIN,
            BENCHMARK=self.benchmark,
            XML=self.xml_config,
            FLAGS=constants.OLTP_DEFAULT_COMMAND_FLAGS,
            RESULTS=self.test_output_file)

    def run_pre_test(self):
        # install the OLTP
        self.install_oltp()
        self.config_xml_file()
        self.create_result_dir()

    def create_result_dir(self):
        if not os.path.exists(constants.OLTP_DIR_TEST_RESULT):
            os.mkdir(constants.OLTP_DIR_TEST_RESULT)

    def install_oltp(self):
        self.clean_oltp()
        self.download_oltp()
        self.build_oltp()

    def clean_oltp(self):
        p = subprocess.Popen(constants.OLTP_GIT_CLEAN_COMMAND,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != constants.ErrorCode.SUCCESS:
            print("Error: unable to clean OLTP repo")
            print(stderr)
            sys.exit(constants.ErrorCode.ERROR)

    def download_oltp(self):
        p = subprocess.Popen(constants.OLTP_GIT_COMMAND,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != constants.ErrorCode.SUCCESS:
            print("Error: unable to git clone OLTP source code")
            print(stderr)
            sys.exit(constants.ErrorCode.ERROR)

    def build_oltp(self):
        for command in constants.OTLP_ANT_COMMANDS:
            p = subprocess.Popen(command,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            if p.returncode != constants.ErrorCode.SUCCESS:
                print(stderr)
                sys.exit(constants.ErrorCode.ERROR)

    def config_xml_file(self):
        xml = ElementTree.parse(self.xml_template)
        root = xml.getroot()
        root.find("dbtype").text = constants.OLTP_DEFAULT_DBTYPE
        root.find("driver").text = constants.OLTP_DEFAULT_DRIVER
        root.find("DBUrl").text = "jdbc:postgresql://{0}:{1}/{2}".format(
            self.db_host, self.db_port,
            self.benchmark)  #host, port and benchmark name
        root.find("username").text = constants.OLTP_DEFAULT_USERNAME
        root.find("password").text = constants.OLTP_DEFAULT_PASSWORD
        root.find("isolation").text = str(self.transaction_isolation)
        root.find("scalefactor").text = str(self.scalefactor)
        root.find("terminals").text = str(self.terminals)
        for work in root.find("works").findall("work"):
            work.find("time").text = str(self.time)
            work.find("rate").text = str(constants.OLTP_DEFAULT_RATE)
            work.find("weights").text = str(self.weights)

        xml.write(self.xml_config)