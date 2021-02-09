#!/usr/bin/python3

import json
import logging
import os
import sys
import traceback

from ..util.constants import LOG, ErrorCode
from . import constants
from .test_case_oltp import TestCaseOLTPBench
from .test_oltpbench import TestOLTPBench
from .utils import parse_command_line_args


def generate_tests(args):
    """
    Generate tests for TestOLTPBench.run().

    Parameters
    ----------
    args : dict
        The result of parse_command_line_args().
        WARNING: NOTE THAT THIS IS MUTATED WITH THE FOLLOWING KEYS:
            collect_mem_info
            server_args
            continue_on_error
        TODO(WAN): Don't do this...

    Returns
    -------
    Tests that can be executed with TestOLTPBench.run().
    """

    def get_test_json_config():
        config_file = os.path.join(os.getcwd(), args.get("config_file"))
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file doesn't exist: {config_file}")
        with open(config_file) as test_suite_config:
            json_config = json.load(test_suite_config)
            if not json_config:
                raise RuntimeError(f"Bad JSON: {config_file}")
        return json_config

    def build_server_metadata():
        server_metadata = test_json.get('env', {})

        max_connection_threads = int(test_json.get('server_args', {}).get(
            'connection_thread_count',
            str(constants.OLTPBENCH_DEFAULT_CONNECTION_THREAD_COUNT)))
        server_metadata['max_connection_threads'] = max_connection_threads

        wal_enable = test_json.get('server_args', {}).get(
            'wal_enable', constants.OLTPBENCH_DEFAULT_WAL_ENABLE)
        if not wal_enable:
            server_metadata['wal_device'] = 'None'

        return server_metadata

    test_json = get_test_json_config()

    # MUTATE args["collect_mem_info"]. All tests collect memory info by default.
    disable_mem_info = args.get("disable_mem_info")
    args["collect_mem_info"] = not disable_mem_info

    # MUTATE args["server_args"].
    server_args = test_json.get("server_args", {})
    if server_args:
        args["server_args"] = server_args

    # MUTATE args["continue_on_error"].
    args["continue_on_error"] = test_json.get(
        "continue_on_error", constants.OLTPBENCH_DEFAULT_CONTINUE_ON_ERROR)

    # Build the test suite.
    test_suite = []
    for testcase in test_json.get("testcases", []):
        base_test = testcase.get("base")
        base_test["server_data"] = build_server_metadata()
        base_test["publish_results"] = args.get("publish_results")
        base_test["publish_username"] = args.get("publish_username")
        base_test["publish_password"] = args.get("publish_password")

        # The config files support looping over parameters,
        # see nightly/nightly.json for an example.
        loop_tests = testcase.get("loop")
        if not loop_tests:
            test_suite.append(TestCaseOLTPBench(base_test))
        else:
            for loop_item in loop_tests:
                combined_config = {**base_test, **loop_item}
                test_suite.append(TestCaseOLTPBench(combined_config))

    return test_suite


if __name__ == "__main__":
    args = parse_command_line_args()

    exit_code = ErrorCode.ERROR
    try:
        tests = generate_tests(args)
        # Because generate_tests MUTATES args, has to come first.
        oltpbench = TestOLTPBench(args)
        exit_code = oltpbench.run(tests)
    except:
        LOG.error("Exception trying to run OLTPBench tests.")
        traceback.print_exc(file=sys.stdout)

    logging.shutdown()
    sys.exit(exit_code)
