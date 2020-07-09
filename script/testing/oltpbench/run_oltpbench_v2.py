#!/usr/bin/python3

import os
import sys
import argparse
import traceback
import json
import itertools

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from oltpbench.test_case_oltp import TestCaseOLTP
from oltpbench.test_oltpbench_v2 import TestOLTPBenchV2

def parse_tracefile(aparser, args):
    aparser.add_argument(
        "tracefile", help="File containing a collection of test cases")
    aparser.add_argument("--db-host", help="DB Hostname")
    aparser.add_argument("--db-port", type=int, help="DB Port")
    aparser.add_argument("--db-output-file", help="DB output log file")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s")
    args = vars(aparser.parse_args(args))

    args_tracefile = args.get("tracefile")

    oltp_test_suite = []

    if args_tracefile:
        tracefile_path = os.path.join(os.getcwd(), args_tracefile)
        if not os.path.exists(tracefile_path):
            msg = "Unable to find OLTPBench trace file '{}'".format(
                args_tracefile)
            raise RuntimeError(msg)

        with open(tracefile_path) as oltp_tracefile:
            oltp_test_suite_json = json.load(oltp_tracefile)

        for oltp_testcase in oltp_test_suite_json["testcases"]:
            oltp_testcase_base = oltp_testcase.get("base")
            oltp_testcase_loop = oltp_testcase.get("loop")

            # if need to loop over parameters, for example terminals = 1,2,4,8,16
            if oltp_testcase_loop:
                # also support the combination of differetn loop options
                loop_dict = {}
                for loop_key in oltp_testcase_loop.keys():
                    loop_dict[loop_key] = oltp_testcase_loop[loop_key]

                keys, values = zip(*loop_dict.items())
                loop_combination_dicts = [
                    dict(zip(keys, v)) for v in itertools.product(*values)]

                for loop_item in loop_combination_dicts:
                    for loop_key in loop_item.keys():
                        oltp_testcase_base[loop_key] = loop_item[loop_key]
                    oltp_test_suite.append(TestCaseOLTP(oltp_testcase_base))
            else:
                # there is no loop 
                oltp_test_suite.append(TestCaseOLTP(oltp_testcase_base))
    return args, oltp_test_suite


def parse_parameters(aparser, args):
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
    test_suite = []

    if args and args[0].endswith(".json"):
        # trace file mode
        args, test_suite = parse_tracefile(aparser, args)
    else:
        # parameter mode
        args, test_suite = parse_parameters(aparser, args)

    try:
        oltpbench = TestOLTPBenchV2(args)
        exit_code = oltpbench.run(test_suite)
    except:
        print("Exception trying to run OLTP Bench tests")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1

    sys.exit(exit_code)
