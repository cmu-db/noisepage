import argparse

from .constants import (DEFAULT_BENCHMARK, DEFAULT_CONNECTION_THREADS,
                        DEFAULT_SCALE_FACTOR, TATP, TPCC, YCSB)
from .log_throughput import log_throughput
from .test_type import TestType


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
    aparser.add_argument("--async-replication",
                         default=False,
                         action="store_true",
                         help=f"Whether or not async replication is enabled, only relevant when test_type is "
                              f"{TestType.PRIMARY.value} and replication is enabled")
    aparser.add_argument("--async-commit",
                         default=False,
                         action="store_true",
                         help="Whether or not WAL async commit is enabled")
    aparser.add_argument("--oltp-benchmark",
                         default=DEFAULT_BENCHMARK,
                         choices=[YCSB, TPCC, TATP],
                         help="Which OLTPBenchmark benchmark to use")
    aparser.add_argument("--oltp-scale-factor",
                         default=DEFAULT_SCALE_FACTOR,
                         help="Scale factor for OLTPBenchmark")
    aparser.add_argument("--log-file",
                         help=f"File containing log record messages to send to replica node. If no file is specified "
                              f"then logs will be generated using OLTPBench. Only relevant when test_type is "
                              f"{TestType.REPLICA.value}")
    aparser.add_argument("--save-generated-log-file",
                         default=False,
                         action="store_true",
                         help=f"If log record messages are generated then this flag causes those messages to be saved "
                              f"to a file, only relevant when test_type is {TestType.REPLICA.value} and --log-file "
                              f"param is not specified")
    aparser.add_argument("--connection-threads",
                         default=DEFAULT_CONNECTION_THREADS,
                         help=f"Number of database connection threads to use, OLTPBench threads will scale accordingly")
    aparser.add_argument("--output-file",
                         help="File to output the metrics results to")

    args = vars(aparser.parse_args())

    log_throughput(TestType(args["test-type"]), args["build_type"], args["replication_enabled"],
                   args["async_replication"], args["async_commit"], args["oltp_benchmark"],
                   int(args["oltp_scale_factor"]), args["log_file"], int(args["connection_threads"]),
                   args["output_file"], args["save_generated_log_file"])


if __name__ == '__main__':
    main()
