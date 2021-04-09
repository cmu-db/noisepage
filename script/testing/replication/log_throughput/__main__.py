import argparse

from .log_throughput import primary_log_throughput


def main():
    aparser = argparse.ArgumentParser(description="Benchmark for log record throughput")
    aparser.add_argument("--build-type",
                         default="debug",
                         choices=["debug", "release", "relwithdebinfo"],
                         help="Build type (default: %(default)s)")
    aparser.add_argument("--replication-enabled",
                         default=False,
                         action="store_true",
                         help="Whether or not replication is enabled (default: %(default)s)")
    aparser.add_argument("--oltp-benchmark",
                         default="tpcc",
                         choices=["tpcc", "tatp"],
                         help="Which OLTP benchmark to use")
    aparser.add_argument("--output-file",
                         help="File to output the metrics results to")

    args = vars(aparser.parse_args())

    primary_log_throughput(args["build_type"], args["replication_enabled"], args["oltp_benchmark"], args["output_file"])


if __name__ == '__main__':
    main()
