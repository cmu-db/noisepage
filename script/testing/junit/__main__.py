#!/usr/bin/env python3

import os
import sys
import argparse
import traceback
from typing import Dict, List

from . import constants

from ..util.test_case import TestCase
from ..util.test_server import TestServer
from ..util.constants import LOG, ErrorCode

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

# Reserved environment variables used by the testing infrastructure
RESERVED_VARS = ["NOISEPAGE_QUERY_MODE", "NOISEPAGE_PREPARE_THRESHOLD"]

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------

# The original set of environment variables at script entry;
# currently, we manually save this environment and later restore
original_env = {}

# -----------------------------------------------------------------------------
# Test Case Definition
# -----------------------------------------------------------------------------


class TestCaseJUnit(TestCase):
    """
    Class to run JUnit tests.
    """

    def __init__(self, args, test_command=constants.JUNIT_TEST_CMD_ALL):
        TestCase.__init__(self, args)
        self.test_command = test_command
        self.test_command_cwd = constants.JUNIT_TEST_DIR
        self.test_output_file = self.args.get(
            "test_output_file", constants.JUNIT_OUTPUT_FILE
        )


# -----------------------------------------------------------------------------
# Argument Parsing
# -----------------------------------------------------------------------------


def map_server_args(server_arg_arr) -> Dict:
    """
    Map server arguments from an array of strings into a dictionary.

    For example,
    Input: ["--arg1=10", "--args2=hello", "--debug"]
    Output: {
                '--arg1': "10",
                '--arg2': "hello",
                '--debug': None,
            }

    Parameters
    ----------
    server_arg_arr : [str]
        The server arguments as an array of strings.
    Returns
    -------
    server_arg_map : dict
        The server arguments in dictionary form.
    """
    server_arg_map = {}
    for server_arg in server_arg_arr:
        if "=" in server_arg:
            key, value = server_arg.split("=", 1)
        else:
            key, value = server_arg, None
        server_arg_map[key] = value
    return server_arg_map


def parse_arguments() -> Dict:
    """
    Parse the command line arguments accepted by the JUnit module.
    """

    parser = argparse.ArgumentParser(description="JUnit runner.")

    parser.add_argument("--db-host", help="DB hostname.")
    parser.add_argument("--db-port", type=int, help="DB port.")
    parser.add_argument("--db-output-file", help="DB output log file.")
    parser.add_argument("--test-output-file", help="Test output log file.")
    parser.add_argument(
        "--build-type",
        default="debug",
        choices=["debug", "release", "relwithdebinfo"],
        help="Build type (default: %(default)s).",
    )
    parser.add_argument(
        "--query-mode", choices=["simple", "extended"], help="Query protocol mode."
    )
    parser.add_argument(
        "--prepare-threshold",
        default=None,
        type=int,
        help="Threshold under the 'extended' query mode.",
    )
    parser.add_argument(
        "--tracefile-test",
        type=str,
        help="The name of a particular tracefile test to run.",
    )
    parser.add_argument(
        "-a",
        "--server-arg",
        default=[],
        action="append",
        help="Server commandline arguments.",
    )

    args = vars(parser.parse_args())

    args["server_args"] = map_server_args(args.get("server_arg"))
    del args["server_arg"]

    return {k: v for k, v in args.items() if v}


# -----------------------------------------------------------------------------
# Output Utilities
# -----------------------------------------------------------------------------


def section_header(title):
    border = "+++ {} +++".format("=" * 100)
    middle = "+++ {:100} +++".format(title)
    return "\n\n{}\n{}\n".format(border, middle)


# -----------------------------------------------------------------------------
# Generic Test Runner
# -----------------------------------------------------------------------------


def run_test(test_server, test_command: str, args: Dict) -> int:
    """
    Run a generic test instance.
    :param test_server The TestServer instance
    :param test_command The command that implements the test
    :param args The dictionary of script arguments
    :return An integer value representing the test return code
    """
    test_case = TestCaseJUnit(args, test_command=test_command)
    errcode = ErrorCode.ERROR
    try:
        errcode = test_server.run([test_case])
    except KeyboardInterrupt:
        LOG.error("KeyboardInterrupt received. Terminating.")
        raise
    except Exception as err:
        LOG.error(f"Exception trying to run {test_command}")
        LOG.error(err)
        LOG.error("================ Python Error Output ==================")
        traceback.print_exc(file=sys.stdout)
    return errcode


# -----------------------------------------------------------------------------
# JUnit Tests
# -----------------------------------------------------------------------------


def run_junit_tests(test_server, args: Dict) -> List:
    """
    Run all JUnit tests.
    :param test_server The TestServer instance
    :param args The dictionary of script arguments
    :return A list of tuples of the test results
    """
    LOG.info(section_header("TEST JUNIT"))
    return [("junit", run_test(test_server, constants.JUNIT_TEST_CMD_JUNIT, args))]


# -----------------------------------------------------------------------------
# Tracefile Tests
# -----------------------------------------------------------------------------


def run_tracefile_test(path: str, test_server, args: Dict) -> int:
    """
    Run an invidual tracefile test.
    :param path The path to the tracefile
    :param test_server The test server instance
    :param args The dictionary of script arguments
    :return A tuple of
        - The tracefile name
        - An integer value representing the return code of the test
    """

    # TODO(WAN): maybe this shouldn't be an env var too : )
    # TODO(Kyle): Yea this seems absolutely insane to be using an environment var for this...

    old_env_var = os.environ.get("NOISEPAGE_TRACE_FILE", None)
    os.environ["NOISEPAGE_TRACE_FILE"] = path

    LOG.info(section_header("TEST TRACEFILE: " + os.environ["NOISEPAGE_TRACE_FILE"]))

    try:
        errcode = run_test(test_server, constants.JUNIT_TEST_CMD_TRACE, args)
    finally:
        if old_env_var is None:
            del os.environ["NOISEPAGE_TRACE_FILE"]
        else:
            os.environ["NOISEPAGE_TRACE_FILE"] = old_env_var

    # NOTE(Kyle): os.path.basename() will fail on non-POSIX systems,
    # but we only support Linux at this point, so I am OK with this
    return (os.path.basename(path), errcode)


def run_tracefile_tests(test_server, args: Dict) -> List:
    """
    Run all tracefiles in the trace file directory.
    :param test_server The TestServer instance
    :param args The dictionary of script arguments
    :return A list of tuples of test results
    """

    base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    trace_dir = os.path.join(base_path, constants.REPO_TRACE_DIR)

    # Locate all trace files in the trace directory; remove those without expected extension
    filenames = [
        name
        for name in os.listdir(trace_dir)
        if name.endswith(constants.TESTFILES_SUFFIX)
    ]
    # Construct the complete relative path to each trace file
    paths = sorted([os.path.join(trace_dir, filename) for filename in filenames])

    # Execute each test and return a collection of the results
    return [run_tracefile_test(path, test_server, args) for path in paths]


# -----------------------------------------------------------------------------
# Environment Management
# -----------------------------------------------------------------------------

# TODO(WAN): Using env vars is dumb and buggy when we control the entire stack.


def set_env_vars(args: Dict):
    """
    Set the environment variables used by the testing infrastructure.
    :param args The script arguments
    """
    new_values = {
        "NOISEPAGE_QUERY_MODE": args.get("query_mode", constants.DEFAULT_QUERY_MODE),
        "NOISEPAGE_PREPARE_THRESHOLD": str(
            args.get("prepare_threshold", constants.DEFAULT_PREPARE_THRESHOLD)
        ),
    }
    for var in RESERVED_VARS:
        original_env[var] = os.environ.get(var, None)
        os.environ[var] = new_values[var]


def unset_env_vars():
    """
    Unset the environment variables updated by the testing infrastructure.
    """
    for var in RESERVED_VARS:
        if original_env[var] is None:
            del os.environ[var]
        else:
            os.environ[var] = original_env[var]


# -----------------------------------------------------------------------------
# Top-Level Test Logic
# -----------------------------------------------------------------------------


def run_specified_test(test_name: str, args: Dict) -> List:
    """
    Run a single, specified integration test.
    :param test_name The name of the test of interest
    :param args The dictionary of script arguments
    :return A list of test results
    """
    test_server = TestServer(args, quiet=True)

    # NOTE(Kyle): Currently, we only support manually specifiying
    # invidual tracefile tests; there is not way to specify an
    # individual JUnit test

    base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    trace_dir = os.path.join(base_path, constants.REPO_TRACE_DIR)

    # Locate all trace files in the trace directory; remove those without expected extension
    filenames = [
        name
        for name in os.listdir(trace_dir)
        if name.endswith(constants.TESTFILES_SUFFIX)
    ]

    if test_name not in filenames:
        raise RuntimeError("Specified tracefile test not found")

    test_path = os.path.join(trace_dir, test_name)
    return [run_tracefile_test(test_path, test_server, args)]


def run_all_tests(args: Dict) -> List:
    """
    Run all integration tests.
    :param args The dictionary of script arguments
    :return A list of test results
    """
    test_server = TestServer(args, quiet=True)
    results = []
    results.extend(run_junit_tests(test_server, args))
    results.extend(run_tracefile_tests(test_server, args))
    return results


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> int:
    args = parse_arguments()

    set_env_vars(args)

    results = []
    try:
        if "tracefile_test" in args:
            results = run_specified_test(args["tracefile_test"], args)
        else:
            results = run_all_tests(args)
    except RuntimeError as e:
        print(e)
    except KeyboardInterrupt:
        traceback.print_exc(file=sys.stderr)
    finally:
        unset_env_vars()

    # Compute the final exit code.
    LOG.info("Tests:")
    final_code = 0 if len(results) > 0 else 1
    for name, errcode in results:
        final_code = final_code or errcode
        LOG.info("\t{} : {}".format("FAIL" if errcode else "SUCCESS", name))

    LOG.info("Final Status => {}".format("FAIL" if final_code else "SUCCESS"))
    return final_code


# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    sys.exit(main())
