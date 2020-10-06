import os
from glob import glob

from micro_bench.gbench_run_result import GBenchRunResult
from micro_bench.gbench_historical_results import GBenchHistoricalResults
from micro_bench.constants import LOCAL_REPO_DIR
from micro_bench.benchmarks import BENCHMARKS_TO_RUN
from util.constants import LOG

class ArtifactProcessor(object):
    """ Loads historical Google benchmark test results. Provide access by 
        (suite_name, test_name)
    """
    def __init__(self, required_num_results=None):
        # key = (suite_name, test_name)
        self.artifacts = {}
        self.required_num_results = required_num_results
        LOG.debug("min_ref_values: {}".format(required_num_results))
        return

    def load_local_artifacts(self, latest_local_build_dir):
        LOG.debug("Processing local data repository {}".format(LOCAL_REPO_DIR))
        local_build_dirs = reversed(sorted(next(os.walk(LOCAL_REPO_DIR))[1]))
        for build_dir in local_build_dirs:
            if os.path.basename(build_dir) == latest_local_build_dir:
                LOG.debug("Skipping data dir {}".format(build_dir))
                continue
            LOG.debug("Reading results from local directory {}".format(build_dir))
            for build_file in glob(os.path.join(LOCAL_REPO_DIR, build_dir,'*.json')):
                self.add_artifact_file(build_file)
                # Determine if we have enough history. Stop collecting information if we do
                if self.has_min_history():
                    return
    
    def has_min_history(self):
        """ check whether all the collected artifacts have at least the minimum
            number of results.
        """
        if not self.required_num_results:
            LOG.debug("required_num_results is not set???")
            return False
        if len(self.artifacts) == 0:
            LOG.debug("No artifacts available")
            return False
        
        for key in self.artifacts.keys():
            artifact = self.artifacts.get(key)
            (suite_name, test_name) = key
            LOG.debug("# of artifacts for {SUITE}.{TEST}: {NUM_ARTIFACTS} [required={REQUIRED_ARTIFACTS}]".format(
                SUITE=suite_name, TEST=test_name, NUM_ARTIFACTS=artifact.get_num_results(), REQUIRED_ARTIFACTS=self.required_num_results))
            if artifact.get_num_results() < self.required_num_results:
                return False
        return True

    def add_artifact_file(self, result_file):
        """ Add an artifact file to the list of files to be used
        for computing summary statistics

        data : raw json data from the artifact file
        """
        gbench_run_result = GBenchRunResult(result_file)

        # iterate over the GBBenchResult objects
        for key, bench_result in gbench_run_result.benchmarks.items():
            # add to a GBBenchResultProcessor
            historical_results = self.artifacts.get(key, GBenchHistoricalResults(*key))
            self.artifacts[key] = historical_results
            if not (self.required_num_results and historical_results.get_num_results() >= self.required_num_results):
                historical_results.add_gbench_test_result(bench_result)
        return

    def get_comparison(self, bench_name, gbench_result, lax_tolerance=None):
        """ create and return a dict comparing the historical benchmark results
            to the benchmark result passed in
        """
        key = (gbench_result.suite_name, gbench_result.test_name)
        historical_results = self.artifacts.get(key,None)
        ref_type = 'none'
        if historical_results:
            if historical_results.get_num_results() >= self.required_num_results:
                ref_type = 'historic'
            else:
                ref_type = 'lax'
        return self._create_comparison_dict(bench_name, gbench_result, ref_type, lax_tolerance)

    def _create_comparison_dict(self, bench_name, gbench_result, ref_type='none', lax_tolerance=None):
        """ creates the comparison dict based on the ref type """
        key = (gbench_result.suite_name, gbench_result.test_name)
        LOG.debug("Loading {REF_TYPE} history data for {SUITE_NAME}.{TEST_NAME} [{BENCH_NAME}]".format(
            REF_TYPE=ref_type, SUITE_NAME=gbench_result.suite_name, TEST_NAME=gbench_result.test_name, BENCH_NAME=bench_name
        ))
        comparison = {
            "suite": gbench_result.suite_name,
            "test": gbench_result.test_name,
            "num_results": 0,
            "throughput": gbench_result.items_per_second,
            "iterations": gbench_result.iterations,
            "tolerance": 0,
            "reference_type": ref_type,
            "status": "PASS"
        }
        if ref_type != 'none':
            historical_results = self.artifacts.get(key)
            comparison['num_results'] = historical_results.get_num_results()
            comparison['tolerance'] = BENCHMARKS_TO_RUN[bench_name] if ref_type == 'historic' else lax_tolerance

            comparison['ref_throughput'] = historical_results.get_mean_throughput()
            comparison['coef_var'] = 100 * historical_results.get_stdev_throughput() / comparison.get('ref_throughput')
            # comparison['time'] = historical_results.get_mean_time() #TODO: lets add time
            comparison['change'] = 100 * (gbench_result.items_per_second - comparison.get('ref_throughput'))/ comparison.get('throughput')
            comparison['status'] = 'PASS' if is_comparison_pass(comparison.get('ref_throughput'),
                                                        gbench_result.items_per_second, 
                                                        comparison.get('tolerance')) else 'FAIL'
        return comparison

def is_comparison_pass(avg_historical_throughput, test_throughput, tolerance, ref_type='none'):
    """ determine whether or not to consider the benchmark test as passed """
    if ref_type == 'none':
        return True
    min_allowed_throughput = avg_historical_throughput - float(avg_historical_throughput) * (float(tolerance)/100)
    return test_throughput < min_allowed_throughput
