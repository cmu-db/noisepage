#!/usr/bin/env python3

import os
import sys
import traceback

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from junit.constants import JUNIT_TEST_CMD_JUNIT, JUNIT_TEST_CMD_TRACE, TESTFILES_PREFIX, REPO_TRACE_DIR
from junit.utils import parse_command_line_args
from junit.test_junit import TestJUnit
from util.constants import LOG, ErrorCode
from test_case_junit import TestCaseJUnit


def section_header(title):
    border = "+++ " + "=" * 100 + " +++\n"
    middle = "+++ " + title.center(100) + " +++\n"
    return "\n\n" + border + middle + border
# DEF


if __name__ == "__main__":

    args = parse_command_line_args()

    all_exit_codes = []
    exit_code = ErrorCode.SUCCESS
    junit_test_runner = TestJUnit(args)

    # Step 1: Run the regular JUnit tests.
    LOG.info(section_header("JUNIT TESTS"))
    try:
        test_case_junit = TestCaseJUnit(args, test_command=JUNIT_TEST_CMD_JUNIT)
        exit_code = junit_test_runner.run(test_case_junit)
    except:
        LOG.error(f'Exception trying to run {JUNIT_TEST_CMD_JUNIT}')
        LOG.error("================ Python Error Output ==================")
        traceback.print_exc(file=sys.stdout)
        exit_code = ErrorCode.ERROR
    finally:
        all_exit_codes.append(exit_code)

    # Step 2: Run the trace test for each file that we find
    # Each directory represents another set of SQL traces to test.
    LOG.info(section_header("TRACEFILE TESTS"))
    noise_trace_dir = os.path.join(base_path, REPO_TRACE_DIR)
    for item in os.listdir(noise_trace_dir):
        # Look for all of the .test files in the each directory
        if item.endswith(TESTFILES_PREFIX):
            os.environ["NOISEPAGE_TRACE_FILE"] = os.path.join(noise_trace_dir, item)
            LOG.info(section_header("TRACEFILE TEST: " + os.environ["NOISEPAGE_TRACE_FILE"]))
            exit_code = ErrorCode.ERROR
            try:
                test_case_junit = TestCaseJUnit(
                    args, test_command=JUNIT_TEST_CMD_TRACE)
                exit_code = junit_test_runner.run(test_case_junit)
            except KeyboardInterrupt:
                exit_code = ErrorCode.ERROR
                raise
            except Exception as err:
                LOG.error(f'Exception trying to run {JUNIT_TEST_CMD_TRACE}')
                LOG.error(err)
                LOG.error("================ Python Error Output ==================")
                traceback.print_exc(file=sys.stdout)
                exit_code = ErrorCode.ERROR
            finally:
                all_exit_codes.append(exit_code)
        ## FOR (files)
    ## FOR (dirs)

    # Compute final exit code. If any test failed, then the entire program has to fail
    final_code = 0
    for c in all_exit_codes:
        final_code = final_code or c
    LOG.info("Final Status => {}".format("FAIL" if final_code else "SUCCESS"))
    sys.exit(final_code)
# MAIN
