#!/usr/bin/env python3

import os
import sys
import traceback

from ..util.constants import LOG, ErrorCode
from ..util.test_server import TestServer
from .constants import (DEFAULT_PREPARE_THRESHOLD, JUNIT_TEST_CMD_JUNIT,
                        JUNIT_TEST_CMD_TRACE, REPO_TRACE_DIR, TESTFILES_SUFFIX)
from .test_case_junit import TestCaseJUnit
from .utils import parse_command_line_args


def section_header(title):
    border = "+++ {} +++".format("=" * 100)
    middle = "+++ {:100} +++".format(title)
    return "\n\n{}\n{}\n".format(border, middle)


def run_test(test_server, test_command):
    test_case = TestCaseJUnit(args, test_command=test_command)
    errcode = ErrorCode.ERROR
    try:
        errcode = test_server.run([test_case])
    except KeyboardInterrupt:
        LOG.error("KeyboardInterrupt received. Terminating.")
        raise
    except Exception as err:
        LOG.error(f'Exception trying to run {test_command}')
        LOG.error(err)
        LOG.error("================ Python Error Output ==================")
        traceback.print_exc(file=sys.stdout)
    return errcode


def run_tests_junit(test_server):
    LOG.info(section_header("TEST JUNIT"))
    return ("junit", run_test(test_server, JUNIT_TEST_CMD_JUNIT))


def run_tests_tracefiles(test_server):
    base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    noise_trace_dir = os.path.join(base_path, REPO_TRACE_DIR)

    def run_tracefile(test_server, filename):
        # TODO(WAN): maybe this shouldn't be an env var too : )
        old_env_var = os.environ.get("NOISEPAGE_TRACE_FILE", None)
        os.environ["NOISEPAGE_TRACE_FILE"] = os.path.join(noise_trace_dir, filename)

        LOG.info(section_header("TEST TRACEFILE: " + os.environ["NOISEPAGE_TRACE_FILE"]))

        try:
            errcode = run_test(test_server, JUNIT_TEST_CMD_TRACE)
        finally:
            if old_env_var is None:
                del os.environ["NOISEPAGE_TRACE_FILE"]
            else:
                os.environ["NOISEPAGE_TRACE_FILE"] = old_env_var

        return errcode

    return [(item, run_tracefile(test_server, item))
            for item in sorted(os.listdir(noise_trace_dir)) if item.endswith(TESTFILES_SUFFIX)]


if __name__ == "__main__":
    args = parse_command_line_args()

    # TODO(WAN): Using env vars is dumb and buggy when we control the entire stack.
    original_env = {}
    special_vars = ["NOISEPAGE_QUERY_MODE", "NOISEPAGE_PREPARE_THRESHOLD"]


    def set_env_vars():
        new_values = {
            "NOISEPAGE_QUERY_MODE": args.get("query_mode", "simple"),
            "NOISEPAGE_PREPARE_THRESHOLD": str(args.get("prepare_threshold", DEFAULT_PREPARE_THRESHOLD)),
        }
        for var in special_vars:
            original_env[var] = os.environ.get(var, None)
            os.environ[var] = new_values[var]


    def unset_env_vars():
        for var in special_vars:
            if original_env[var] is None:
                del os.environ[var]
            else:
                os.environ[var] = original_env[var]


    errcodes = []
    try:
        set_env_vars()
        test_server = TestServer(args, quiet=True)
        errcodes = [run_tests_junit(test_server)] + run_tests_tracefiles(test_server)
    except KeyboardInterrupt:
        traceback.print_exc(file=sys.stderr)
    finally:
        unset_env_vars()

    # Compute the final exit code.
    LOG.info("Tests:")
    final_code = 0 if len(errcodes) > 0 else 1
    for (item, errcode) in errcodes:
        final_code = final_code or errcode
        LOG.info("\t{} : {}".format("FAIL" if errcode else "SUCCESS", item))

    LOG.info("Final Status => {}".format("FAIL" if final_code else "SUCCESS"))
    sys.exit(final_code)
