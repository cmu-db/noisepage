import os
from glob import glob

from microbench.google_benchmark.gbench_run_result import GBenchRunResult
from microbench.google_benchmark.gbench_historical_results import GBenchHistoricalResults
from microbench.jenkins.jenkins import Jenkins
from microbench.constants import LOCAL_REPO_DIR, JENKINS_URL
from microbench.benchmarks import BENCHMARKS_TO_RUN
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
        """ Load artifacts when run in local mode.

        It reads the results from the local directory structure and stores
        them in the artifact processor.
        
        Parameters
        ----------
        latest_local_build_dir : str
            The path to the latest local build dir. The directories increment 
            001, 002, 003, etc. This is the highest one.
        """
        LOG.debug("Processing local data repository {}".format(LOCAL_REPO_DIR))
        local_build_dirs = reversed(sorted(next(os.walk(LOCAL_REPO_DIR))[1]))
        for build_dir in local_build_dirs:
            if os.path.basename(build_dir) == latest_local_build_dir:
                LOG.debug("Skipping data dir {}".format(build_dir))
                continue
            LOG.debug("Reading results from local directory {}".format(build_dir))
            for build_file in glob(os.path.join(LOCAL_REPO_DIR, build_dir, '*.json')):
                gbench_run_results = GBenchRunResult.from_benchmark_file(build_file)
                self.add_artifact(gbench_run_results)
                # Determine if we have enough history. Stop collecting information if we do
                if self.has_min_history():
                    return

    def load_jenkins_artifacts(self, ref_data_source):
        """ Load the Jenkins artifacts into the artifact processor.
        
        Parameters
        ----------
        ref_data_source : dict
            An object that contains information about where to get the Jenkins
            data. It could be a project or a branch within that project.
        """
        jenkins = Jenkins(JENKINS_URL)
        folders = ref_data_source.get("folders", [])
        project = ref_data_source.get("project")
        branch = ref_data_source.get("branch", None)
        min_build = ref_data_source.get("min_build", 0)
        status_filter = ref_data_source.get("status_filter")
        for artifact in jenkins.get_artifacts(folders, project, branch, min_build, status_filter):
            gbench_run_results = GBenchRunResult(artifact)
            self.add_artifact(gbench_run_results)
            # Determine if we have enough history. Stop collecting information if we do
            if self.has_min_history():
                return

    def has_min_history(self):
        """ Check whether all the collected artifacts have at least the minimum
        number of results.

        Returns
        -------
        bool
            Whether there were enough historical results.
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

    def add_artifact(self, gbench_run_result):
        """ Add an artifact file to the list of files to be used
        for computing summary statistics.

        Paramters
        ---------
        gbench_run_result : GBenchRunResult
            The Google Benchmark results from a single run. This will be added
            to the list of artifacts.
        """
        # iterate over the GBBenchResult objects
        for key, bench_result in gbench_run_result.benchmarks.items():
            # add to a GBBenchResultProcessor
            historical_results = self.artifacts.get(
                key, GBenchHistoricalResults(*key))
            self.artifacts[key] = historical_results
            if not (self.required_num_results and historical_results.get_num_results() >= self.required_num_results):
                historical_results.add_gbench_test_result(bench_result)
        return

    def get_comparison_for_publish_result(self, bench_name, gbench_result, lax_tolerance=None):
        """ Create and return a dict comparing the historical benchmark results
        to the benchmark result passed in.
        
        This will format the results in a way that the performance storage
        service will understand. The main difference between this method and
        get_comparison is stdev_throughput.

        Parameters
        ----------
        bench_name : str
            The name of the microbenchmark.
        gbench_result : GBenchRunResult
            The benchmark result to be compared against.
        lax_tolerance : int, optional
            The allowed level of tolerance for performance. This is only used
            if there are not enough historical results. (The default is None).

        Returns
        -------
        publishable_comparison : dict
            An object with information about the microbenchmark results and how
            it compares to the historical results.
        """
        initial_comparison = self.get_comparison(bench_name, gbench_result, lax_tolerance)
        fields = ['suite', 'test', 'throughput', 'tolerance',
                  'status', 'iterations', 'ref_throughput', 'num_results']

        publishable_comparison = {
            key: value for key, value in initial_comparison.items() if key in fields}

        key = (gbench_result.suite_name, gbench_result.test_name)
        historical_results = self.artifacts.get(key)
        if initial_comparison.get('reference_type') != 'none':
            publishable_comparison['stdev_throughput'] = historical_results.get_stdev_throughput()

        return publishable_comparison

    def get_comparison(self, bench_name, gbench_result, lax_tolerance=None):
        """ Compare the historical benchmark results to the benchmark result
        passed in.

        This will determine what type of comparison ref_type it should be based
        on the number of historical results.

        Parameters
        ----------
        bench_name : str
            The name of the microbenchmark.
        gbench_result : GBenchRunResult
            The result to compare against.
        lax_tolerance : int, optional
            The allowed level of tolerance for performance. This is only used
            if there are not enough historical results. (The default is None).

        Returns
        -------
        dict
            An object with information about the microbenchmark results and how
            it compares to the historical results.
        """
        key = (gbench_result.suite_name, gbench_result.test_name)
        historical_results = self.artifacts.get(key, None)
        ref_type = 'none'
        if historical_results:
            if historical_results.get_num_results() >= self.required_num_results:
                ref_type = 'historic'
            else:
                ref_type = 'lax'
        return self._create_comparison_dict(bench_name, gbench_result, ref_type, lax_tolerance)

    def _create_comparison_dict(self, bench_name, gbench_result, ref_type='none', lax_tolerance=None):
        """ Creates the comparison dict based on the ref type.
        
        It takes a GBenchTestResult and compares it against the results for the
        same benchmark stored in the artifact processor.
        
        Parameters
        ----------
        bench_name : str
            The name of the microbenchmark.
        gbench_result : GBenchRunResult
            The result to compare against.
        ref_type : str, optional
            The type of comparison to do against the historical reference data.
            The options are 'none', 'lax', 'historic'. 'none' does no
            comparison, 'lax' uses a relaxed threshold, and 'historic' uses
            normal thresholds. (The default is 'none').
        lax_tolerance : int, optional
            The allowed level of tolerance for performance. This is only used
            if there are not enough historical results. (The default is None).

        Returns
        -------
        dict
            An object with information about the microbenchmark results and how
            it compares to the historical results.
        """
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
            if historical_results.get_mean_throughput() <= 0 or \
               not comparison.get('throughput') or comparison.get('throughput') <= 0:
                return comparison

            comparison['num_results'] = historical_results.get_num_results()
            comparison['tolerance'] = BENCHMARKS_TO_RUN[bench_name] if ref_type == 'historic' else lax_tolerance
            comparison['ref_throughput'] = historical_results.get_mean_throughput()
            comparison['coef_var'] = 100 * historical_results.get_stdev_throughput() / comparison.get('ref_throughput')
            comparison['change'] = 100 * (gbench_result.items_per_second -
                                          comparison.get('ref_throughput')) / comparison.get('ref_throughput')
            comparison['status'] = 'PASS' if is_comparison_pass(comparison.get('ref_throughput'),
                                                                comparison.get('throughput'),
                                                                comparison.get('tolerance')) else 'FAIL'
        return comparison


def is_comparison_pass(avg_historical_throughput, test_throughput, tolerance, ref_type='none'):
    """ Determine whether or not to consider the benchmark test as passed.
    
    This is based on whether the throughput of the microbenchmark has decreased
    more than the allowed tolerance %.
    
    Parameters
    ----------
    avg_historical_throughput : int
        The average of the historical results' throughput.
    test_throughput : int
        The throughput of the microbenchmark test result.
    tolerance : int
        The percentage decrease allowed in throughput.
    ref_type : str, optional
        The type of comparison to do against the historical reference data.
        The options are 'none', 'lax', 'historic'. 'none' does no
        comparison. Otherwise, a normal comparison is done. (The default is
        'none').

    Returns
    -------
    bool
        True if the test_throughput is within the allowed limits or if no
        comparison should be done (ref_type = 'none'). False if the throughput
        has degraded beyond the allowed threashold.
    """
    if ref_type == 'none':
        return True
    min_allowed_throughput = avg_historical_throughput - float(avg_historical_throughput) * (float(tolerance) / 100)
    return test_throughput > min_allowed_throughput
