"""
ling bench
"""

import os
import sys
import csv
import argparse
import subprocess
import logging
from pprint import pprint

# Where to find the benchmarks to execute
benchmark_path = "build/release/tpcc_benchmark"
# The path to the logfile for the benchmarks.
BENCHMARK_LOGFILE_PATH = "/tmp/benchmark_ling.log"

def run_single_benchmark(num_worker_threads):
	""" Run benchmark, generate JSON results
	"""
	output_file = "coor_{}worker_result.json".format(str(num_worker_threads))	 

	cmd = "{} --benchmark_format=json" + \
		  " --benchmark_out={}"
	cmd = cmd.format(benchmark_path, output_file)

	# Environment Variables
	os.environ["TERRIER_BENCHMARK_THREADS"] = str(num_worker_threads) # has to be a str
	os.environ["TERRIER_BENCHMARK_LOGFILE_PATH"] = BENCHMARK_LOGFILE_PATH

	output = subprocess.check_output("numactl --hardware | grep 'available: ' | cut -d' ' -f2", shell=True)
	if not output:
		raise Exception("Missing numactl binary. Please install package")
	highest_cpu_node = int(output) - 1
	print("Number of NUMA Nodes = {}".format(highest_cpu_node))

	cmd = "numactl --cpunodebind={} --preferred={} {}".format(highest_cpu_node, highest_cpu_node, cmd)
	print("Executing command [num_worker={}]: {}".format(num_worker_threads, cmd))

	proc = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = proc.communicate()
	ret_val = proc.returncode

	if ret_val != 0:
		print(err)	
			

worker_thread_count = sys.argv[1]


run_single_benchmark(worker_thread_count)
