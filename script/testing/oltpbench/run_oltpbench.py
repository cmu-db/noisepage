#!/usr/bin/python3

import os
import sys
import traceback
import json
import itertools

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from oltpbench.test_case_oltp import TestCaseOLTPBench
from oltpbench.test_oltpbench import TestOLTPBench
from oltpbench.utils import parse_command_line_args
from oltpbench import constants
from util.constants import LOG

def generate_test_suite(args):
    args_configfile = args.get("config_file")

    oltp_test_suite = []

    if not args_configfile:
        msg = "config file is not provided"
        raise FileNotFoundError(msg)

    configfile_path = os.path.join(os.getcwd(), args_configfile)
    if not os.path.exists(configfile_path):
        msg = "Unable to find OLTPBench config file '{}'".format(
            args_configfile)
        raise FileNotFoundError(msg)

    oltp_test_suite_json = None
    with open(configfile_path) as oltp_tracefile:
        oltp_test_suite_json = json.load(oltp_tracefile)

    if not oltp_test_suite_json:
        msg = "Unable to load OLTPBench config file '{}'. Maybe it is not valid JSON file?".format(
            args_configfile)
        raise TypeError(msg)

    max_connection_threads = constants.OLTPBENCH_DEFAULT_CONNECTION_THREAD_COUNT

    # read server commandline args in config files
    server_args_json = oltp_test_suite_json.get("server_args")
    if server_args_json:
        server_args = ""
        for attribute,value in server_args_json.items():
            server_args = '{SERVER_ARGS} -{ATTRIBUTE}={VALUE}'.format(SERVER_ARGS=server_args,ATTRIBUTE=attribute,VALUE=value)
            
            #Delete the logfile before each run
            if attribute == "log_file_path":
                old_log_path = str(value)
                if os.path.exists(old_log_path):
                    os.remove(old_log_path)
            
            #Update connection_thread_count if user override
            if attribute == "connection_thread_count":
                max_connection_threads=int(value)

        args["server_args"] = server_args

    # read metadata in config file
    server_data = oltp_test_suite_json.get("env")
    server_data["max_connection_threads"] = max_connection_threads
    
    # publish test results to the server
    oltp_report_server = constants.PERFORMANCE_STORAGE_SERVICE_API[args.get("publish_results")]
    oltp_report_username = args.get("publish_username")
    oltp_report_password= args.get("publish_password")

    for oltp_testcase in oltp_test_suite_json.get("testcases", []):
        oltp_testcase_base = oltp_testcase.get("base")

        oltp_testcase_base["server_data"] = server_data

        oltp_testcase_base["publish_results"] = oltp_report_server
        oltp_testcase_base["publish_username"] = oltp_report_username
        oltp_testcase_base["publish_password"] = oltp_report_password
        oltp_testcase_loop = oltp_testcase.get("loop")

        # if need to loop over parameters, for example terminals = 1,2,4,8,16
        if oltp_testcase_loop:
            for loop_item in oltp_testcase_loop:
                oltp_testcase_combined = {**oltp_testcase_base,**loop_item}             
                oltp_test_suite.append(TestCaseOLTPBench(oltp_testcase_combined)) 

        else:
            # there is no loop
            oltp_test_suite.append(TestCaseOLTPBench(oltp_testcase_base))

    oltpbench = TestOLTPBench(args)
    return oltpbench, oltp_test_suite


if __name__ == "__main__":
    
    args = parse_command_line_args()
    try:
        oltpbench, test_suite = generate_test_suite(args) 
        exit_code = oltpbench.run(test_suite)
    except:
        LOG.error("Exception trying to run OLTP Bench tests")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1

    sys.exit(exit_code)
