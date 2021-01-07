import argparse


def parse_command_line_args():
    """
    Parse the command line arguments accepted by the JUnit module.
    """

    aparser = argparse.ArgumentParser(description="JUnit runner.")

    aparser.add_argument("--db-host", help="DB hostname.")
    aparser.add_argument("--db-port", type=int, help="DB port.")
    aparser.add_argument("--db-output-file", help="DB output log file.")
    aparser.add_argument("--test-output-file", help="Test output log file.")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s).")
    aparser.add_argument("--query-mode",
                         choices=["simple", "extended"],
                         help="Query protocol mode.")
    aparser.add_argument("--prepare-threshold",
                         default=None,
                         type=int,
                         help="Threshold under the 'extended' query mode.")
    aparser.add_argument('-a', "--server-arg",
                         default=[],
                         action='append',
                         help="Server commandline arguments.")

    args = vars(aparser.parse_args())

    args['server_args'] = map_server_args(args.get('server_arg'))
    del args['server_arg']
    return args


def map_server_args(server_arg_arr):
    server_arg_map = {}
    for server_arg in server_arg_arr:
        if '=' in server_arg:
            key, value = server_arg.split('=', 1)
        else:
            key, value = server_arg, None
        server_arg_map[key] = value
    return server_arg_map
