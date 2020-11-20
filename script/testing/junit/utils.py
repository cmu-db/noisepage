#!/usr/bin/python3

import argparse

from oltpbench import constants

def parse_command_line_args():
    '''Command line argument parsing methods'''

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
                         help="Threshold under the 'extended' query mode")
    aparser.add_argument("--server-args",
                         help="Server Commandline Args")

    args = vars(aparser.parse_args())

    return args
