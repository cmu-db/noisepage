import json
import os
from datetime import datetime

from micro_bench.gbench_test_result import GBenchTestResult
from util.constants import LOG

class GBenchRunResult(object):
    """ The results from running a Google benchmark file (i.e. bwtree_benchmark)
        This may contain multiple individual benchmark tests. This class will
        parse the results and create a map of benchmark test results where the
        key is (suite_name, test_name) and the value is a GBenchTestResult.
    """
    def __init__(self, result_file_name):
        self.benchmarks = {}
        LOG.debug("Loading local benchmark result file {}".format(result_file_name))
        with open(result_file_name) as result_file:
            try:
                raw_results = result_file.read()
                self.results = json.loads(raw_results)
            except Exception as err:
                LOG.error("Invalid data read from benchmark result file {}".format(result_file_name))
                LOG.error(err)
                LOG.error(raw_results)
                raise 
        self._populate_benchmark_results(self.results)
        self.benchmark_name = self._get_benchmark_name()


    def _populate_benchmark_results(self, results_dict):
        """ populates the benchmark map with GBenchTestResult objects and adds
            a timestamp from the higher level JSON into the individual test
            result
        """
        for bench in results_dict.get('benchmarks',[]):
            bench['timestamp'] = self._get_datetime()
            test_result = GBenchTestResult(bench)
            key = (test_result.suite_name, test_result.test_name)
            self.benchmarks[key] = test_result
        return

    def _get_datetime(self):
        """ Return formatted datetime from the top-level JSON """
        date_format = "%Y-%m-%dT%H:%M:%S"
        default_date = datetime.now().strftime(date_format)+'+00:00'
        date_str = self.results.get('context',{}).get('date', default_date)
        return datetime.strptime(date_str[:-6], date_format)

    def _get_benchmark_name(self):
        """ get benchmark name from the file path """
        return os.path.basename(self.results.get('context').get('executable'))



