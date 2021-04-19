import argparse

from .constants import DEFAULT_LOG_RECORD_MESSAGES_FILE, DEFAULT_CONNECTION_THREADS, DEFAULT_BENCHMARK, \
    DEFAULT_SCALE_FACTOR, TATP, TPCC, YCSB
from .log_throughput import log_throughput
from .test_type import TestType
from ...util.constants import LOG


def main():
    aparser = argparse.ArgumentParser(description="Benchmark for log record throughput",
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    aparser.add_argument("test-type",
                         choices=[TestType.PRIMARY.value, TestType.REPLICA.value],
                         help="Indicates whether to measure log throughput on primary or replica nodes")
    aparser.add_argument("--build-type",
                         default="release",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type")
    aparser.add_argument("--replication-enabled",
                         default=False,
                         action="store_true",
                         help=f"Whether or not replication is enabled, only relevant when test_type is "
                              f"{TestType.PRIMARY.value}")
    aparser.add_argument("--replication-policy",
                         default="sync",
                         choices=["sync", "async"],
                         help="WARNING: This is not currently implemented. You have to manually change the policy in "
                              "db_main.h right now")
    aparser.add_argument("--async-commit",
                         default=False,
                         action="store_true",
                         help="Whether or not WAL async commit is enabled")
    aparser.add_argument("--oltp-benchmark",
                         default=DEFAULT_BENCHMARK,
                         choices=[YCSB, TPCC, TATP],
                         help=f"Which OLTP benchmark to use, only relevant when test_type is {TestType.PRIMARY.value}")
    aparser.add_argument("--oltp-scale-factor",
                         default=DEFAULT_SCALE_FACTOR,
                         help=f"Scale factor for OLTP benchmark, only relevant when test_type is "
                              f"{TestType.PRIMARY.value}")
    aparser.add_argument("--log-file",
                         default=DEFAULT_LOG_RECORD_MESSAGES_FILE,
                         help=f"File containing log record messages to send to replica node, only relevant when "
                              f"test_type is {TestType.REPLICA.value}")
    aparser.add_argument("--connection-threads",
                         default=DEFAULT_CONNECTION_THREADS,
                         help=f"Number of database connection threads to use, OLTP threads will scale accordingly")
    aparser.add_argument("--output-file",
                         help="File to output the metrics results to")

    args = vars(aparser.parse_args())

    test_type = TestType(args["test-type"])
    log_file = args["log_file"]

    if test_type.value == TestType.REPLICA.value and log_file == DEFAULT_LOG_RECORD_MESSAGES_FILE:
        LOG.warn(f"\n\nWARNING: the default log file {DEFAULT_LOG_RECORD_MESSAGES_FILE} likely doesn't have enough "
                 f"messages to provide accurate results. If you want more accurate results please generate a larger "
                 f"log file using the log scraper script.\n\n")

    log_throughput(test_type, args["build_type"], args["replication_enabled"], args["async_commit"],
                   args["oltp_benchmark"], int(args["oltp_scale_factor"]), log_file, int(args["connection_threads"]),
                   args["output_file"])


if __name__ == '__main__':
    main()
