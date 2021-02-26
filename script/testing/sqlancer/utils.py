import argparse

from ..util.constants import PERFORMANCE_STORAGE_SERVICE_API


def parse_command_line_args():
    """
    Parse the command line arguments accepted by the OLTPBench module.
    """

    aparser = argparse.ArgumentParser(description="Sqlancer runner")

    aparser.add_argument("--db-host", help="DB Hostname.")
    aparser.add_argument("--db-port", type=int, help="DB Port.")
    aparser.add_argument("--db-output-file", help="DB output log file.")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s")

    args = vars(aparser.parse_args())

    return args
