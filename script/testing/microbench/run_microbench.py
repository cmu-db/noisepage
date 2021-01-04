import os
import sys
import argparse
import logging
import json

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from microbench.artifact_processor import ArtifactProcessor
from microbench.micro_benchmarks_runner import MicroBenchmarksRunner
from microbench.config import Config
from microbench.results_output import send_results, table_dump
from microbench.constants import (BENCHMARK_THREADS, BENCHMARK_LOGFILE_PATH, BENCHMARK_PATH, MIN_REF_VALUES)
from util.constants import LOG, PERFORMANCE_STORAGE_SERVICE_API

# =========================================================
# MAIN
# =========================================================


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('benchmark',
                        nargs='*',
                        help="Benchmark suite to run [default=ALL]")

    parser.add_argument("--local",
                        action="store_true",
                        default=False,
                        help="Store results in local directory")

    parser.add_argument("--num-threads",
                        metavar='N',
                        type=int,
                        default=BENCHMARK_THREADS,
                        help="# of threads to use for benchmarks")

    parser.add_argument("--logfile-path",
                        metavar='P',
                        type=str,
                        default=BENCHMARK_LOGFILE_PATH,
                        help="Path to use for benchmark WAL files")

    parser.add_argument("--min-ref-values",
                        metavar='M',
                        type=int,
                        default=MIN_REF_VALUES,
                        help="Minimal # of values needed to enforce threshold")

    parser.add_argument("--benchmark-path",
                        metavar='B',
                        type=str,
                        default=BENCHMARK_PATH,
                        help="Path to benchmark binaries")

    parser.add_argument("--csv-dump",
                        action="store_true",
                        default=False,
                        help="Print results to stdout as CSV")

    parser.add_argument("--debug",
                        action="store_true",
                        dest="debug",
                        default=False,
                        help="Enable debug output")

    parser.add_argument("--perf",
                        action="store_true",
                        default=False,
                        help="Enable perf counter recording")

    parser.add_argument("--folders",
                        type=str,
                        nargs="*",
                        help="The jenkins subfolder from which to fetch the Jenkins artifacts in order to compare the results against")

    parser.add_argument("--branch",
                        type=str,
                        help="The branch from which to fetch the Jenkins artifacts in order to compare the results against")

    parser.add_argument("--publish-results",
                        default="none",
                        type=str,
                        choices=PERFORMANCE_STORAGE_SERVICE_API.keys(),
                        help="Environment in which to store performance results")

    parser.add_argument("--publish-username",
                        type=str,
                        help="Performance Storage Service Username")

    parser.add_argument("--publish-password",
                        type=str,
                        help="Performance Storage Service password")

    args = parser.parse_args()

    # -------------------------------------------------------
    # Set config
    # -------------------------------------------------------

    if args.debug:
        LOG.setLevel(logging.DEBUG)
    LOG.debug("args: {}".format(args))

    config_args = {
        'publish_results_env': args.publish_results
    }
    if args.num_threads:
        config_args['num_threads'] = args.num_threads
    if args.logfile_path:
        config_args['logfile_path'] = args.logfile_path
    if args.benchmark_path:
        config_args['benchmark_path'] = args.benchmark_path
    if args.local:
        config_args['is_local'] = args.local
    if args.benchmark:
        config_args['benchmarks'] = sorted(args.benchmark)
    if args.folders:
        config_args['jenkins_folders'] = args.folders
    if args.branch:
        config_args['branch'] = args.branch
    if args.publish_results != 'none':
        if (not args.publish_username) or (not args.publish_password):
            LOG.error("Error: --publish-username and --publish-password are required to publish the results.")
            sys.exit(1)
        config_args['publish_results_username'] = args.publish_username
        config_args['publish_results_password'] = args.publish_password

    config = Config(**config_args)

    # -------------------------------------------------------
    # Execute tests
    # -------------------------------------------------------

    ret_code = 0
    benchmark_runner = MicroBenchmarksRunner(config)
    ret_code = benchmark_runner.run_benchmarks(args.perf)

    if args.local and not ret_code:
        benchmark_runner.create_local_dirs()

    # -------------------------------------------------------
    # Process results
    # -------------------------------------------------------
    if not ret_code:
        artifact_processor = ArtifactProcessor(args.min_ref_values)
        if args.local:
            artifact_processor.load_local_artifacts(
                benchmark_runner.last_build)
        else:
            artifact_processor.load_jenkins_artifacts(config.ref_data_source)

        if args.csv_dump:
            LOG.error("--csv-dump is not currently supported")
        else:
            table_dump(config, artifact_processor)

        if args.publish_results != 'none':
            ret_code = send_results(config, artifact_processor)

    LOG.debug("Exit code = {}".format(ret_code))
    sys.exit(ret_code)
