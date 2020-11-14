#!/usr/bin/python3

import os
import sys
import traceback
import json
import itertools
import logging

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from oltpbench.test_case_oltp import TestCaseOLTPBench
from oltpbench.test_oltpbench import TestOLTPBench
from oltpbench.utils import parse_command_line_args
from oltpbench import constants
from util.constants import LOG


def generate_test_suite(args):

    oltp_test_suite = []
    configfile_path = get_configfile_path(args)
    test_suite_json = load_test_suite(configfile_path)

    # All OLTPBench test collect memory info by default.
    disable_mem_info = args.get("disable_mem_info")
    args["collect_mem_info"] = not disable_mem_info

    max_connection_threads = int(
        test_suite_json.get('server_args', {}).get(
            'connection_thread_count',
            str(constants.OLTPBENCH_DEFAULT_CONNECTION_THREAD_COUNT)))

    wal_enable = test_suite_json.get('server_args', {}).get(
        'wal_enable', constants.OLTPBENCH_DEFAULT_WAL_ENABLE)
    # read server commandline args in config files
    server_args = get_server_args(test_suite_json)
    if (server_args):
        args['server_args'] = server_args

    # read metadata in config file
    server_metadata = get_server_metadata(test_suite_json,
                                          max_connection_threads, wal_enable)

    # if one of test cases failed, whether the script should stop the whole testing or continue
    args["continue_on_error"] = test_suite_json.get(
        "continue_on_error", constants.OLTPBENCH_DEFAULT_CONTINUE_ON_ERROR)

    publish_env, publish_username, publish_password = get_test_result_publish_data(
        args)
    for oltp_testcase in test_suite_json.get("testcases", []):
        oltp_testcase_base = oltp_testcase.get("base")
        oltp_testcase_base["server_data"] = server_metadata
        oltp_testcase_base["publish_results"] = publish_env
        oltp_testcase_base["publish_username"] = publish_username
        oltp_testcase_base["publish_password"] = publish_password
        oltp_testcase_loop = oltp_testcase.get("loop")

        # if need to loop over parameters, for example terminals = 1,2,4,8,16
        if oltp_testcase_loop:
            for loop_item in oltp_testcase_loop:
                oltp_testcase_combined = {**oltp_testcase_base, **loop_item}
                oltp_test_suite.append(
                    TestCaseOLTPBench(oltp_testcase_combined))

        else:
            # there is no loop
            oltp_test_suite.append(TestCaseOLTPBench(oltp_testcase_base))

    oltpbench = TestOLTPBench(args)
    return oltpbench, oltp_test_suite


def get_configfile_path(args):
    """ Return the path to the test suite config file """
    args_configfile = args.get('config_file')
    if not args_configfile:
        raise FileNotFoundError('Config file is not provided')
    configfile_path = os.path.join(os.getcwd(), args_configfile)
    if not os.path.exists(configfile_path):
        raise FileNotFoundError(
            'Config file path does not exist {PATH}'.format(
                PATH=configfile_path))
    return configfile_path


def load_test_suite(configfile_path):
    """ Load the test suite from a JSON config file """
    with open(configfile_path) as test_suite_config:
        test_suite_json = json.load(test_suite_config)

    if not test_suite_json:
        raise TypeError(
            'Unable to load {PATH}. Check if it is valid JSON.'.format(
                PATH=configfile_path))
    return test_suite_json


def get_server_args(test_suite_json):
    """ Create a server args string to pass to the DBMS """
    server_args_json = test_suite_json.get('server_args')

    if server_args_json:
        server_args = ''
        for attribute, value in server_args_json.items():
            server_args = '{SERVER_ARGS} -{ATTRIBUTE}={VALUE}'.format(
                SERVER_ARGS=server_args, ATTRIBUTE=attribute, VALUE=value)

            #Delete the logfile before each run
            if attribute == 'wal_file_path':
                previous_logfile_path = str(value)
                if os.path.exists(previous_logfile_path):
                    os.remove(previous_logfile_path)

        return server_args


def get_server_metadata(test_suite_json, max_connection_threads, wal_enable):
    """ Aggregate all the server metadata in one dictionary """
    server_metadata = test_suite_json.get('env', {})
    server_metadata['max_connection_threads'] = max_connection_threads
    if not wal_enable:
        server_metadata['wal_device'] = 'None'
    return server_metadata


def get_test_result_publish_data(args):
    """ Get the data needed to publish the results, from the args. """
    publish_env = args.get("publish_results")
    publish_username = args.get("publish_username")
    publish_password = args.get("publish_password")
    return publish_env, publish_username, publish_password


if __name__ == "__main__":
    args = parse_command_line_args()
    try:
        oltpbench, test_suite = generate_test_suite(args)
        exit_code = oltpbench.run(test_suite)
    except:
        LOG.error("Exception trying to run OLTP Bench tests")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1
    logging.shutdown()
    sys.exit(exit_code)
