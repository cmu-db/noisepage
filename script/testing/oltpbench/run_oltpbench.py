#!/usr/bin/python3

import os
import sys
import argparse
import traceback

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from oltpbench.test_oltpbench import TestOLTPBench

if __name__ == "__main__":
    aparser = argparse.ArgumentParser(description="Timeseries")

    aparser.add_argument("benchmark", help="Benchmark Type")
    aparser.add_argument("weights", help="Benchmark weights")
    aparser.add_argument("--db-host", help="DB Hostname")
    aparser.add_argument("--db-port", type=int, help="DB Port")
    aparser.add_argument("--db-output-file", help="DB output log file")
    aparser.add_argument("--scale-factor", type=float, metavar="S", \
                         help="The scale factor. (default: 1)")
    aparser.add_argument("--transaction-isolation", metavar="I", \
                         help="The transaction isolation level (default: TRANSACTION_SERIALIZABLE")
    aparser.add_argument("--client-time", type=int, metavar="C", \
                         help="How long to execute each benchmark trial (default: 20)")
    aparser.add_argument("--terminals", type=int, metavar="T", \
                         help="Number of terminals in each benchmark trial (default: 1)")
    aparser.add_argument("--loader-threads", type=int, metavar="L", \
                         help="Number of loader threads to use (default: 1)")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s")
    args = vars(aparser.parse_args())

    try:
        oltpbench = TestOLTPBench(args)
        exit_code = oltpbench.run()
    except:
        print("Exception trying to run OLTP Bench tests")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1

    sys.exit(exit_code)
