import argparse

from ..util.constants import PERFORMANCE_STORAGE_SERVICE_API


def parse_command_line_args():
    """
    Parse the command line arguments accepted by the OLTPBench module.
    """

    aparser = argparse.ArgumentParser(description="Timeseries")

    aparser.add_argument("--config-file",
                         help="File containing a collection of test cases.",
                         required=True)
    aparser.add_argument("--db-host", help="DB Hostname.")
    aparser.add_argument("--db-port", type=int, help="DB Port.")
    aparser.add_argument("--db-output-file", help="DB output log file.")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s")

    public_report_server_list = PERFORMANCE_STORAGE_SERVICE_API.keys()
    aparser.add_argument("--publish-results",
                         choices=public_report_server_list,
                         help="Stores performance results in TimeScaleDB")
    aparser.add_argument("--publish-username",
                         default="none",
                         help="Publish Username")
    aparser.add_argument("--publish-password",
                         default="none",
                         help="Publish password")
    aparser.add_argument("--server-args", help="Server Commandline Args")
    aparser.add_argument("--disable-mem-info",
                         action='store_true',
                         help="Disable collecting the memory info")
    aparser.add_argument("--dry-run",
                         action='store_true',
                         help="Start and stop DB server without running the OLTPBench tests")
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
