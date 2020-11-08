import os
import sys
import argparse
import logging

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from binary_metrics.base_binary_metrics_collector import BaseBinaryMetricsCollector
from binary_metrics.binary_metrics_collectors import *
from util.constants import LOG

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--debug",
                        action="store_true",
                        dest="debug",
                        default=False,
                        help="Enable debug output")

    args = parser.parse_args()

    if args.debug:
        LOG.setLevel(logging.DEBUG)

    # Get the BaseBinaryMetricsCollector subclasses imported from binary_metrics.binary_metrics_collectors
    # Effectively this adds each binary metric collector class into an array to be instantiated later.
    collectors = [obj for obj in BaseBinaryMetricsCollector.__subclasses__()]

    aggregated_metrics = {}
    exit_code = 0
    try:
        for collector in collectors:
            collector_instance = collector(args.debug)
            LOG.info("Starting {COLLECTOR_NAME} collection".format(COLLECTOR_NAME=collector_instance.__class__.__name__))

            LOG.debug("Running setup...")
            collector_instance.setup()

            LOG.debug("Collecting metrics...")
            exit_code = collector_instance.run_collector()

            LOG.debug("Running teardown...")
            collector_instance.teardown()

            results = collector_instance.get_metrics()
            aggregated_metrics.update(results)

            if exit_code:
                LOG.error("{COLLECTOR_NAME} failed. Stopping all binary metric collection".format(COLLECTOR_NAME=collector.__class__.__name__))
                break
            LOG.info("{COLLECTOR_NAME} finished successfully".format(COLLECTOR_NAME=collector_instance.__class__.__name__))
    except Exception as err:
        exit_code = 1
        LOG.error(err)

    LOG.debug("Binary metrics results: {}".format(aggregated_metrics))
    #TODO: send to performance storage service

    logging.shutdown()
    sys.exit(exit_code) 