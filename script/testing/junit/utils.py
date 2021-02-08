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

    args = {k: v for k, v in args.items() if v}
    return args


def map_server_args(server_arg_arr):
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
        if '=' in server_arg:
            key, value = server_arg.split('=', 1)
        else:
            key, value = server_arg, None
        server_arg_map[key] = value
    return server_arg_map
