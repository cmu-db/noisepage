
import os
import sys
import argparse
import logging
import json

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from micro_bench.config import Config
from micro_bench.micro_benchmarks_runner import MicroBenchmarksRunner
from micro_bench.artifact_processor import ArtifactProcessor
from micro_bench.text_table import TextTable
from micro_bench.gbench_run_result import GBenchRunResult
from util.constants import LOG, PERFORMANCE_STORAGE_SERVICE_API
from micro_bench.constants import (JENKINS_URL, LOCAL_REPO_DIR, BENCHMARK_THREADS, 
                                    BENCHMARK_LOGFILE_PATH, BENCHMARK_PATH, MIN_REF_VALUES)

def table_dump(config, artifact_processor):
    #TODO: This function could use some work
    text_table = TextTable()
    ret = 0
    for bench_name in sorted(config.benchmarks):
        filename = "{}.json".format(bench_name)
        bench_results = GBenchRunResult(filename)

        for key in sorted(bench_results.benchmarks.keys()):
            result = bench_results.benchmarks.get(key)
            LOG.debug("%s Result:\n%s", bench_name, result)

            comparison = artifact_processor.get_comparison(bench_name, result, config.lax_tolerance)
            if comparison.get('pass') == 'FAIL':
                ret = 1
            text_table.add_row(comparison)

    text_table.add_column("status")
    text_table.add_column("iterations")
    text_table.add_column("throughput", col_format="{:1.4e}")
    text_table.add_column("ref_throughput", heading="ref throughput", col_format="{:1.4e}")
    text_table.add_column("tolerance", heading="%tolerance")
    text_table.add_column("change", heading="%change", col_format="{:+.0f}")
    text_table.add_column("coef_var",heading="%coef var", col_format="{:.0f}")
    text_table.add_column("reference_type", heading="ref type")
    text_table.add_column("num_results", heading="#results")
    text_table.add_column("suite")
    text_table.add_column("test")
    print("")
    print(text_table)

    return (ret)    

## =========================================================
## MAIN
## =========================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('benchmark',
                    nargs='*',
                    help="Benchmark suite to run [default=ALL]")

    parser.add_argument("--run",
                        action="store_true",
                        dest="run",
                        default=False,
                        help="Run Benchmarks")
    
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

    args = parser.parse_args()

    # -------------------------------------------------------

    if args.debug: LOG.setLevel(logging.DEBUG)
    LOG.debug("args: {}".format(args))

    config_args = {}
    if args.num_threads: config_args['num_threads'] = args.num_threads
    if args.logfile_path: config_args['logfile_path'] = args.logfile_path
    if args.benchmark_path: config_args['benchmark_path'] = args.benchmark_path
    if args.local: config_args['is_local'] = args.local
    if args.benchmark: config_args['benchmarks'] = sorted(args.benchmark)

    config = Config(**config_args)
    ret_code = 0 # TODO: I don't think I need an exit code here cause it always gets overwritten at the end
    if args.run:
        benchmark_runner = MicroBenchmarksRunner(config)
        #ret_code = benchmark_runner.run_benchmarks(args.perf)
        ret_code = benchmark_runner.run_benchmarks(args.perf)

        if args.local and not ret_code:
            # TODO: If local I think I want to repeat the run if args.min_ref_values is not satisfied
            # That will be a future enhancement though
            benchmark_runner.create_local_dirs()

    if not ret_code:
        # Artifact processor
        artifact_processor = ArtifactProcessor(args.min_ref_values)
        if args.local:
            artifact_processor.load_local_artifacts(benchmark_runner.last_build)
        
        if args.csv_dump:
            #ret_code = csv_dump(config.benchmarks, artifact_processor)
            LOG.error("Not supported yet")
        else:
            ret_code = table_dump(config, artifact_processor)

    LOG.debug("Exit code = {}".format(ret_code))
    sys.exit(ret_code)



