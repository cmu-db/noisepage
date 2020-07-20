#!/usr/bin/env python3

import os
import sys
import argparse
import traceback

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from junit import constants
from junit.test_junit import TestJUnit
from util.constants import LOG

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

    all_exit_codes = []
    
    # Step 1: Run the regular JUnit tests. 
    exit_code = 0
    try:
        runner = TestJUnit(args)
        runner.test_command = constants.JUNIT_TEST_CMD_JUNIT
        exit_code = runner.run()
    except:
        LOG.error("Exception trying to run '%s'" % constants.JUNIT_TEST_CMD_JUNIT)
        LOG.error("================ Python Error Output ==================")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1
    finally:
        all_exit_codes.append(exit_code)

    # Step 2: Run the trace test for each file that we find
    # Each directory represents another set of SQL traces to test.
    noise_trace_dir = os.path.join(base_path, constants.JUNIT_DIR, constants.REPO_TRACE_DIR)
    for item in os.listdir(noise_trace_dir):
        if item.endswith(constants.TESTFILES_PREFIX):
        # Look for all of the .test files in the each directory
            os.environ["NOISEPAGE_TRACE_FILE"] = os.path.join(noise_trace_dir, item)
            LOG.info(os.environ["NOISEPAGE_TRACE_FILE"])
            exit_code = 0
            try:
                runner = TestJUnit(args)
                runner.test_command = constants.JUNIT_TEST_CMD_TRACE
                exit_code = runner.run()
            except KeyboardInterrupt:
                exit_code = 1
                raise
            except:
                LOG.error("Exception trying to run '%s'" % constants.JUNIT_TEST_CMD_TRACE)
                LOG.error("================ Python Error Output ==================")
                traceback.print_exc(file=sys.stdout)
                exit_code = 1
            finally:
                all_exit_codes.append(exit_code)
            LOG.info("="*80)
        ## FOR (files)
    ## FOR (dirs)
    
    # Compute final exit code. If any test failed, then the entire program has to fail

    final_code = 0
    for c in all_exit_codes:
        final_code = final_code or c

    sys.exit(final_code)
# MAIN