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
                         default = None,
                         type=int,
                         help="Threshold under the 'extended' query mode")
    aparser.add_argument('-a',"--server-arg",
                         type=str,
                         nargs='*',
                         help="Server Commandline Args")

    args = vars(aparser.parse_args())

    args['server-args'] = map_server_args(args.get('server_arg',[]))
    del args['server_arg']
    return args

def map_server_args(server_arg_arr):
    server_arg_map = {}
    for server_arg in server_arg_arr:
        key, value = server_arg.split('=',1)
        server_arg_map[key] = value
    return server_arg_map
