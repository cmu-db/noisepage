#!/usr/bin/env python

"""
Run micro-benchmarks on a PR, for the purpose of comparing performance with
the master branch.

Usage:
From the top level of the terrier build tree
./script/micro_bench/run_micro_bench.py
"""

import os
import subprocess
import sys

class RunMicroBenchmarks(object):
    """ Run micro benchmarks. Output is to json files for post processing.
        Returns True if all benchmarks run, False otherwise
    """
    def __init__(self):
        # list of benchmarks to run
        self.benchmark_list = ["data_table_benchmark",
                               "tuple_access_strategy_benchmark"]

        # minimum run time for the benchmark
        self.min_time = 10
        return

    def run_all_benchmarks(self):
        """ Return True on success of all benchmarks
                   False if any fail
        """
        ret_val = True

        # iterate over all benchmarks and run them
        for benchmark_name in self.benchmark_list:
            bench_ret_val = self.run_single_benchmark(benchmark_name)
            if bench_ret_val:
                ret_val = False

        # return fail, if any of the benchmarks failed to run or complete
        return ret_val

    def run_single_benchmark(self, benchmark_name):
        """ Run benchmark, generate JSON results
        """
        benchmark_path = os.path.join("./release/release", benchmark_name)
        output_file = "{}_out.json".format(benchmark_name)

        cmd = "{} --benchmark_min_time={} " + \
              " --benchmark_format=json" + \
              " --benchmark_out={}"
        cmd = cmd.format(benchmark_path,
                         self.min_time,
                         output_file)

        ret_val = subprocess.call([cmd],
                                  shell=True,
                                  stdout=sys.stdout,
                                  stderr=sys.stderr)

        # return the process exit code
        return ret_val

if __name__ == "__main__":
    run_bench = RunMicroBenchmarks()
    ret = run_bench.run_all_benchmarks()
    sys.exit(ret)

