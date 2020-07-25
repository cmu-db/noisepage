#!/usr/bin/python3

import os
import sys
import traceback
import json
import itertools

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from oltpbench.test_case_oltp import TestCaseOLTP
from oltpbench.test_oltpbench_v2 import TestOLTPBenchV2
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

    # publish test results to the server
    oltp_report_server = constants.PERFORMANCE_STORAGE_SERVICE_API[args.get("publish_results")]
    oltp_report_username = args.get("publish_username")
    oltp_report_password= args.get("publish_password")

    for oltp_testcase in oltp_test_suite_json.get("testcases", []):
        oltp_testcase_base = oltp_testcase.get("base")

        oltp_testcase_base["publish_results"] = oltp_report_server
        oltp_testcase_base["publish_username"] = oltp_report_username
        oltp_testcase_base["publish_password"] = oltp_report_password
        oltp_testcase_loop = oltp_testcase.get("loop")

        # if need to loop over parameters, for example terminals = 1,2,4,8,16
        if oltp_testcase_loop:
            for loop_item in oltp_testcase_loop:
                oltp_testcase_combined = {**oltp_testcase_base,**loop_item}             
                oltp_test_suite.append(TestCaseOLTP(oltp_testcase_combined)) 

        else:
            # there is no loop
            oltp_test_suite.append(TestCaseOLTP(oltp_testcase_base))

    return oltp_test_suite


if __name__ == "__main__":
    
    args = parse_command_line_args()

    test_suite = generate_test_suite(args)

    try:
        oltpbench = TestOLTPBenchV2(args)
        exit_code = oltpbench.run(test_suite)
    except:
        LOG.error("Exception trying to run OLTP Bench tests")
        traceback.print_exc(file=sys.stdout)
        exit_code = 1

    sys.exit(exit_code)
