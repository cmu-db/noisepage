import json
import os
import time
from xml.etree import ElementTree

from ..reporting.report_result import report_oltpbench_result
from ..util.common import run_command
from ..util.constants import INCREMENTAL_METRIC_FREQ, LOG, ErrorCode
from ..util.test_case import TestCase
from . import constants


class TestCaseOLTPBench(TestCase):
    """Class of a test case of OLTPBench"""

    def __init__(self, args):
        TestCase.__init__(self, args)

        self._set_oltpbench_attributes(args)
        self._init_test_case()

    def _set_oltpbench_attributes(self, args):
        """
        Parse the arguments to set all the relevant OLTPBench attributes.

        Parameters
        ----------
        args : dict
            The result of parse_command_line_args().
        """
        self.benchmark = str(args.get("benchmark"))
        self.scalefactor = float(args.get("scale_factor", constants.OLTPBENCH_DEFAULT_SCALEFACTOR))
        self.terminals = int(args.get("terminals", constants.OLTPBENCH_DEFAULT_TERMINALS))
        self.loader_threads = int(args.get("loader_threads", constants.OLTPBENCH_DEFAULT_LOADER_THREADS))
        self.client_time = int(args.get("client_time", constants.OLTPBENCH_DEFAULT_TIME))
        self.weights = str(args.get("weights"))
        self.transaction_isolation = str(
            args.get("transaction_isolation", constants.OLTPBENCH_DEFAULT_TRANSACTION_ISOLATION))
        self.query_mode = args.get("query_mode")
        self.db_restart = args.get("db_restart", constants.OLTPBENCH_DEFAULT_DATABASE_RESTART)
        self.db_create = args.get("db_create", constants.OLTPBENCH_DEFAULT_DATABASE_CREATE)
        self.db_load = args.get("db_load", constants.OLTPBENCH_DEFAULT_DATABASE_LOAD)
        self.db_execute = args.get("db_execute", constants.OLTPBENCH_DEFAULT_DATABASE_EXECUTE)
        self.buckets = args.get("buckets", INCREMENTAL_METRIC_FREQ)
        self.server_data = args.get("server_data")

        self.publish_results = args.get("publish_results", constants.OLTPBENCH_DEFAULT_REPORT_SERVER)
        self.publish_username = args.get("publish_username")
        self.publish_password = args.get("publish_password")

    def _init_test_case(self):
        # oltpbench xml file paths
        xml_file = "{}_config.xml".format(self.benchmark)
        self.xml_config = os.path.join(constants.OLTPBENCH_DIR_CONFIG, xml_file)
        self.xml_template = os.path.join(constants.OLTPBENCH_DIR_CONFIG, "sample_{}".format(xml_file))

        # for different testing, oltpbench needs different folder to put testing results
        self.filename_suffix = "{BENCHMARK}_{QUERYMODE}_{TERMINALS}_{SCALEFACTOR}_{CLIENTTIME}_{STARTTIME}".format(
            BENCHMARK=self.benchmark,
            QUERYMODE=self.query_mode,
            TERMINALS=self.terminals,
            SCALEFACTOR=self.scalefactor,
            CLIENTTIME=self.client_time,
            STARTTIME=time.strftime("%Y%m%d-%H%M%S"))

        # base directory for the result files, default is in the 'oltp_result' folder under the current directory
        self.test_result_base_dir = self.args.get("test_result_dir")
        if not self.test_result_base_dir:
            self.test_result_base_dir = os.path.join(os.getcwd(),
                                                     "oltp_result")

        # after the script finishes, this director will include the generated files: expconfig, summary, etc.
        self.test_result_dir = os.path.join(self.test_result_base_dir,
                                            self.filename_suffix)

        # oltpbench test results
        self.test_output_file = self.args.get("test_output_file")
        if not self.test_output_file:
            self.test_output_file = os.path.join(self.test_result_dir,
                                                 "oltpbench.log")

        # oltpbench historgrams results - json format
        self.test_histograms_json_file = self.args.get("test_json_histograms")
        if not self.test_histograms_json_file:
            self.test_histograms_json_file = "oltp_histograms_" + self.filename_suffix + ".json"
        self.test_histogram_path = os.path.join(
            constants.OLTPBENCH_GIT_LOCAL_PATH, self.test_histograms_json_file)

        # oltpbench initiate database and load data
        self.oltp_flag = "--histograms --execute={EXECUTE} -s {BUCKETS}".format(
            EXECUTE=self.db_execute, BUCKETS=self.buckets)

        # oltpbench test command
        self.test_command = "{BIN} -b {BENCHMARK} -c {XML} -d {RESULTS} {FLAGS} -json-histograms {HISTOGRAMS}".format(
            BIN=constants.OLTPBENCH_DEFAULT_BIN,
            BENCHMARK=self.benchmark,
            RESULTS=self.test_result_dir,
            XML=self.xml_config,
            FLAGS=self.oltp_flag,
            HISTOGRAMS=self.test_histogram_path)
        self.test_command_cwd = constants.OLTPBENCH_GIT_LOCAL_PATH

    def run_pre_test(self):
        self._config_xml_file()
        self._create_result_dir()
        self._create_and_load_db()

    def run_post_test(self):
        # validate the OLTP result
        self._validate_result()

        # publish results
        if self.publish_results:
            report_oltpbench_result(
                self.publish_results, self.server_data,
                os.path.join(os.getcwd(), "oltp_result",
                             self.filename_suffix), self.publish_username,
                self.publish_password, self.mem_metrics, self.query_mode)

    def _create_result_dir(self):
        """
        Create the directory for the result output files if not exists.
        """
        if not os.path.exists(self.test_result_dir):
            os.makedirs(self.test_result_dir)

    def _create_and_load_db(self):
        """
        Create the database and load the data before the actual test execution.
        """
        cmd = "{BIN} -c {XML} -b {BENCHMARK} --create={CREATE} --load={LOAD}".format(
            BIN=constants.OLTPBENCH_DEFAULT_BIN,
            XML=self.xml_config,
            BENCHMARK=self.benchmark,
            CREATE=self.db_create,
            LOAD=self.db_load)
        rc, stdout, stderr = run_command(cmd,
                                         cwd=self.test_command_cwd)
        if rc != ErrorCode.SUCCESS:
            LOG.info(stdout.read())
            LOG.error(stderr.read())
            raise RuntimeError("Error: Unable to create and load the database.")

    def _get_db_url(self):
        db_url = f"jdbc:postgresql://{self.db_host}:{self.db_port}/noisepage"
        db_url += f"?preferQueryMode={self.query_mode}" if self.query_mode else ""
        return db_url

    def _config_xml_file(self):
        xml = ElementTree.parse(self.xml_template)
        root = xml.getroot()
        root.find("dbtype").text = constants.OLTPBENCH_DEFAULT_DBTYPE
        root.find("driver").text = constants.OLTPBENCH_DEFAULT_DRIVER
        root.find("DBUrl").text = self._get_db_url()
        root.find("username").text = constants.OLTPBENCH_DEFAULT_USERNAME
        root.find("password").text = constants.OLTPBENCH_DEFAULT_PASSWORD
        root.find("isolation").text = str(self.transaction_isolation)
        root.find("scalefactor").text = str(self.scalefactor)
        root.find("terminals").text = str(self.terminals)
        for work in root.find("works").findall("work"):
            work.find("time").text = str(self.client_time)
            work.find("rate").text = str(constants.OLTPBENCH_DEFAULT_RATE)
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

    def _validate_result(self):
        """
        Verify that the results do not indicate an error in the OLTPBench test.

        Raises
        -------
        FileNotFoundError
            If self.test_histogram_path does not exist.
        RuntimeError
            If the self.test_histogram_path file has unexpected results.
        """

        # Make sure the file exists before we try to open it.
        # If it's not there, we'll dump out the contents of the directory to make it
        # easier to determine whether or not we are crazy when running Jenkins.
        if not os.path.exists(self.test_histogram_path):
            print("=" * 50)
            print("Directory Contents: {}".format(os.path.dirname(self.test_histogram_path)))
            print("\n".join(os.listdir(os.path.dirname(self.test_histogram_path))))
            print("=" * 50)
            raise FileNotFoundError(f"Unable to find OLTPBench result file: {self.test_histogram_path}")

        with open(self.test_histogram_path) as oltp_result_file:
            test_result = json.load(oltp_result_file)
        unexpected_result = test_result.get("unexpected", {}).get("HISTOGRAM")
        if unexpected_result and unexpected_result.keys():
            for test in unexpected_result.keys():
                if unexpected_result[test] != 0:
                    raise RuntimeError(str(unexpected_result))
        else:
            raise RuntimeError(str(unexpected_result))
