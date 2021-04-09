import argparse

from .log_throughput import primary_log_throughput


def main():
    aparser = argparse.ArgumentParser(description="Benchmark for log record throughput")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s)")
    # TODO this isn't parsing right
    aparser.add_argument("--replication-enabled",
                         default=False,
                         choices=[True, False],
                         help="Whether or not replication is enabled (default: %(default)s)")
    aparser.add_argument("--oltp-benchmark",
                         default="tpcc",
                         choices=["tpcc", "tatp"],
                         help="Which OLTP benchmark to use")

    args = vars(aparser.parse_args())

    primary_log_throughput(args["build_type"], bool(args["replication_enabled"]), args["oltp_benchmark"])


if __name__ == '__main__':
    main()

# Replication: Average log throughput is 5.808376530551583 per millisecond
# No Replication:
