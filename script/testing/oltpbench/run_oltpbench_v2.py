#!/usr/bin/python3

import os
import sys
import argparse
import traceback
import json

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from oltpbench.test_case_oltp import TestCaseOLTP
from oltpbench.test_oltpbench_v2 import TestOLTPBenchV2

def parse_arg_by_trace_file(aparser, args):
    aparser.add_argument(
        "tracefile", help="File containing a trace of certain testcases")
    aparser.add_argument("--db-host", help="DB Hostname")
    aparser.add_argument("--db-port", type=int, help="DB Port")
    aparser.add_argument("--db-output-file", help="DB output log file")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s")
    args = vars(aparser.parse_args(args))

    args_tracefile = args.get("tracefile")

    oltp_testsuite = []

    if args_tracefile:
        tracefile_path = os.path.join(os.getcwd(), args_tracefile)
        if not os.path.exists(tracefile_path):
            msg = "Unable to find OLTPBench trace file '{}'".format(
                args_tracefile)
            raise RuntimeError(msg)

        with open(tracefile_path) as oltp_tracefile:
            oltp_testsuite_json = json.load(oltp_tracefile)

        for oltp_testcase in oltp_testsuite_json["testcases"]:
            oltp_testcase_base = oltp_testcase.get("base")
            oltp_testcase_loop = oltp_testcase.get("loop")

            # if need to loop over parameters, for example terminals = 1,2,4,8,16
            if oltp_testcase_loop and oltp_testcase_loop.get("key"):
                loop_key = oltp_testcase_loop.get("key")
                for loop_value in oltp_testcase_loop.get("values"):
                    oltp_testcase_base[loop_key] = loop_value
                    oltp_testsuite.append(TestCaseOLTP(oltp_testcase_base))
            else:
                oltp_testsuite.append(TestCaseOLTP(oltp_testcase_base))
    return args, oltp_testsuite


def parse_arg_by_args(aparser, args):
    aparser.add_argument("benchmark", help="Benchmark Type")
    aparser.add_argument("weights", help="Benchmark weights")
    aparser.add_argument("--db-host", help="DB Hostname")
    aparser.add_argument("--db-port", type=int, help="DB Port")
    aparser.add_argument("--db-output-file", help="DB output log file")

    aparser.add_argument("--scale-factor", type=float, metavar="S",
                         help="The scale factor. (default: 1)")
    aparser.add_argument("--transaction-isolation", metavar="I",
                         help="The transaction isolation level (default: TRANSACTION_SERIALIZABLE")
    aparser.add_argument("--client-time", type=int, metavar="C",
                         help="How long to execute each benchmark trial (default: 20)")
    aparser.add_argument("--terminals", type=int, metavar="T",
                         help="Number of terminals in each benchmark trial (default: 1)")
    aparser.add_argument("--loader-threads", type=int, metavar="L",
                         help="Number of loader threads to use (default: 1)")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s")
    aparser.add_argument("--query-mode",
                         default="simple",
                         choices=["simple", "extended"],
                         help="Query protocol mode")

    args = vars(aparser.parse_args(args))

    oltp_testcase = TestCaseOLTP(args)

    return args, [oltp_testcase]


if __name__ == "__main__":

    args = sys.argv[1:]
    aparser = argparse.ArgumentParser(description="Timeseries")
    testcases = []

    if args and args[0].endswith(".json"):
        # trace file mode
        args, testcases = parse_arg_by_trace_file(aparser, args)
    else:
        # parameter mode
        args, testcases = parse_arg_by_args(aparser, args)

    try:
        oltpbench = TestOLTPBenchV2(args)
        exit_code = oltpbench.run(testcases)
    except:
        print("Exception trying to run OLTP Bench tests")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1

    sys.exit(exit_code)
