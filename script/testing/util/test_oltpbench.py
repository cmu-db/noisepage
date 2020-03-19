#!/usr/bin/python
import subprocess
from util import constants
from util.test_server import TestServer
from xml.etree import ElementTree


class TestOLTPBench(TestServer):
    """ Class to run OLTP Bench tests """
    def __init__(self, args):
        TestServer.__init__(self, args)
        self.db_host = str(self.args.get())
        self.db_port = constants.DEFAULT_DB_PORT

        # oltpbench specific attributes
        self.benchmark = str(self.args.get("benchmark"))
        self.scalefactor = int(
            self.args.get("scalefactor", constants.OLTP_DEFAULT_SCALEFACTOR))
        self.terminals = int(
            self.args.get("terminals", constants.OLTP_DEFAULT_TERMINAL))
        self.time = int(
            self.args.get("client_time", constants.OLTP_DEFAULT_TIME))
        self.weights = str(self.args.get("weights"))
        self.transaction_isolation = str(
            self.args.transaction_isolation(
                "transaction_isolation",
                constants.OLTP_DEFAULT_TRANSACTION_ISOLATION))

        # oltpbench xml file paths
        self.xml_template = "{}sample_{}_config.xml".format(
            constants.OLTP_DIR_CONFIG, self.benchmark)
        self.xml_config = "{}{}_config.xml".format(constants.OLTP_DIR_CONFIG,
                                                   self.benchmark)

        # oltpbench test results
        self.result_path = "outputfile_{WEIGHTS}_{SCALEFACTOR}".format(
            WEIGHTS=self.weights.replace(",", "_"),
            SCALEFACTOR=str(self.scalefactor))
        self.test_output_file = os.path.join(constants.OLTP_DIR_TEST_RESULT,
                                             self.result_path)

        # oltpbench test command
        self.test_command = "{BIN} -b {BENCHMARK} -c {XML} {FLAGS} -o {RESULTS}".format(
            BIN=constants.OLTP_DEFAULT_BIN,
            BENCHMARK=self.benchmark,
            XML=self.xml_config,
            FLAGS=constants.OLTP_DEFAULT_COMMAND_FLAGS,
            RESULTS=self.result_path)

    def run_pre_test(self):
        # install the OLTP
        self.install_oltp()
        self.config_xml_file()
        self.create_result_dir()

    def create_result_dir(self):
        if not os.path.exists(constants.OLTP_DIR_TEST_RESULT):
            os.mkdir(constants.OLTP_DIR_TEST_RESULT)

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
        root.find("isolation").text = self.transaction_isolation
        root.find("scalefactor").text = self.scalefactor
        root.find("terminals").text = self.terminals
        for work in root.find("works").findall("work"):
            work.find("time").text = str(self.time)
            work.find("rate").text = constants.OLTP_DEFAULT_RATE
            work.find("weights").text = str(self.weight)

        xml.write(self.xml_config)

    def get_result_path(weights, scalefactor):
        return "outputfile_" + weights.replace(",",
                                               "_") + "_" + str(scalefactor)
