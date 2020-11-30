import os
import sys

from microbench.benchmarks import BENCHMARKS_TO_RUN
from util.constants import LOG
from microbench.constants import (LAX_TOLERANCE, MIN_TIME, BENCHMARK_THREADS,
                                  BENCHMARK_PATH, BENCHMARK_LOGFILE_PATH, LOCAL_REPO_DIR, JENKINS_REF_PROJECT)


class Config(object):
    """ Configuration for run_micro_bench. All information is read-only. """

    def __init__(self, benchmark_path=BENCHMARK_PATH, benchmarks=BENCHMARKS_TO_RUN, lax_tolerance=LAX_TOLERANCE, min_time=MIN_TIME,
                 num_threads=BENCHMARK_THREADS, logfile_path=BENCHMARK_LOGFILE_PATH, is_local=False, jenkins_folders=[], branch=None, publish_results_env='none',
                 publish_results_username=None, publish_results_password=None):

        validate_benchmark_path(benchmark_path)
        # path to benchmark binaries
        self.benchmark_path = benchmark_path

        validate_benchmarks(benchmarks)
        # benchmark executables to run
        self.benchmarks = sorted(benchmarks)

        # if fewer than min_ref_values are available
        self.lax_tolerance = lax_tolerance

        # minimum run time for the benchmark, seconds
        self.min_time = min_time

        # the number of threads to use for running microbenchmarks
        self.num_threads = num_threads

        self.logfile_path = logfile_path

        # if local run is specified make sure the local repo is set up
        self.is_local = is_local
        if self.is_local:
            if not os.path.exists(LOCAL_REPO_DIR):
                os.mkdir(LOCAL_REPO_DIR)

        self.ref_data_source = {
            "folders": jenkins_folders,
            "project": JENKINS_REF_PROJECT,
            "branch": branch
        }

        # Environment to which the microbenchmark results will be published (primarily 'prod')
        self.publish_results_env = publish_results_env

        # Credentials needed to send results to the performance storage service
        self.publish_results_username = publish_results_username
        self.publish_results_password = publish_results_password
        return


def validate_benchmark_path(benchmark_path):
    if not os.path.exists(benchmark_path):
        LOG.error("The benchmark executable path directory {} does not exist".format(
            benchmark_path))
        sys.exit(1)


def validate_benchmarks(benchmarks):
    for benchmark in benchmarks:
        if not benchmark.endswith("_benchmark"):
            LOG.error("Invalid target benchmark {}".format(benchmark))
            sys.exit(1)
