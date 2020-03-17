#!/usr/bin/env python

import os
import sys
import argparse
import traceback

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from util.test_junit import TestJUnit

if __name__ == "__main__":

    aparser = argparse.ArgumentParser(description="junit runner")

    aparser.add_argument('--build_type',
                         default="debug",
                         choices=['debug', 'release'],
                         help="Build type (default: %(default)s")

    args = vars(aparser.parse_args())

    # Make it so that we can invoke the script from any directory.
    # Actual execution has to be from the junit directory, so first
    # determine the absolute directory path to this script
    prog_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    # cd to the junit directory
    os.chdir(prog_dir)

    try:
        junit = TestJUnit(args)
        exit_code = junit.run()
    except:
        print("Exception trying to run junit tests")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1

    sys.exit(exit_code)
