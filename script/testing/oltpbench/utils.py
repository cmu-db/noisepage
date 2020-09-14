#!/usr/bin/python3

import argparse

from oltpbench import constants

def parse_command_line_args():
    '''Command line argument parsing methods'''

    aparser = argparse.ArgumentParser(description="Timeseries")

    aparser.add_argument(
        "--config-file", help="File containing a collection of test cases")
    aparser.add_argument("--db-host", help="DB Hostname")
    aparser.add_argument("--db-port", type=int, help="DB Port")
    aparser.add_argument("--db-output-file", help="DB output log file")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s")
    
    public_report_server_list = constants.PERFORMANCE_STORAGE_SERVICE_API.keys()
    aparser.add_argument("--publish-results",
                         default="none",
                         choices=public_report_server_list,
                         help="Stores performance results in TimeScaleDB")
    aparser.add_argument("--publish-username", 
                        default="none",
                        help="Publish Username")
    aparser.add_argument("--publish-password", 
                        default="none",
                        help="Publish password")
    aparser.add_argument("--server-args",
                        help="Server Commandline Args")

    args = vars(aparser.parse_args())

    return args

