import argparse

from .constants import DEFAULT_LOG_RECORD_MESSAGES_FILE
from .log_throughput import log_throughput
from .test_type import TestType


def main():
    aparser = argparse.ArgumentParser(description="Benchmark for log record throughput")
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
                         help="Whether or not replication is enabled, only relevant when test_type is primary")
    aparser.add_argument("--replication-policy",
                         default="sync",
                         choices=["sync", "async"],
                         help="WARNING: This is not currently implemented. You have to manually change the policy in "
                              "db_main.h right now")
    aparser.add_argument("--oltp-benchmark",
                         default="tpcc",
                         choices=["tpcc", "tatp"],
                         help="Which OLTP benchmark to use, only relevant when test_type is primary")
    aparser.add_argument("--log-file",
                         default=DEFAULT_LOG_RECORD_MESSAGES_FILE,
                         help="File containing log record messages to send to replica node, only relevant when "
                              "test_type is replica")

    aparser.add_argument("--output-file",
                         help="File to output the metrics results to")

    args = vars(aparser.parse_args())

    log_throughput(TestType(args["test-type"]), args["build_type"], args["replication_enabled"], args["oltp_benchmark"],
                   args["log_file"], args["output_file"])


if __name__ == '__main__':
    main()
