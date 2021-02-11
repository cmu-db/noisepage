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

    args = vars(aparser.parse_args())

    return args
