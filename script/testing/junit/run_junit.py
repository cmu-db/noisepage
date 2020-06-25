#!/usr/bin/python3

import os
import sys
import argparse
import traceback

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from junit.test_junit import TestJUnit

if __name__ == "__main__":

    aparser = argparse.ArgumentParser(description="junit runner")

    aparser.add_argument("--db-host", help="DB Hostname")
    aparser.add_argument("--db-port", type=int, help="DB Port")
    aparser.add_argument("--db-output-file", help="DB output log file")
    aparser.add_argument("--test-output-file", help="Test output log file")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s)")
    aparser.add_argument("--query-mode",
                         choices=["simple", "extended"],
                         help="Query protocol mode")
    aparser.add_argument("--prepare-threshold",
                         type=int,
                         help="Threshold under the 'extened' query mode")

    args = vars(aparser.parse_args())

    try:
        junit = TestJUnit(args)
        exit_code = junit.run()
    except:
        print("Exception trying to run junit tests")
        print("================ Python Error Output ==================")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1

    sys.exit(exit_code)
