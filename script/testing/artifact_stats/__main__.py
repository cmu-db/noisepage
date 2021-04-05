#!/usr/bin/env python3
import argparse
import logging
import sys

from ..reporting.report_result import report_artifact_stats_result
from ..util.constants import LOG, PERFORMANCE_STORAGE_SERVICE_API
from .base_artifact_stats_collector import BaseArtifactStatsCollector
from .collectors import *  # Import necessary for __subclasses__ enumeration.


def collect_artifact_stats(collectors):
    """
    Executes and combines the results of all the provided collectors.

    Parameters
    ----------
    collectors : [BaseArtifactStatsCollector]

    Returns
    -------
    exit_code : int
        The exit code of the collection task. 0 on success.
    metrics : dict
        The combined metrics from all of the collectors.
        Meaningless if the exit code is not 0.
    """
    exit_code, aggregated_metrics = 0, {}

    try:
        for collector_class in collectors:
            with collector_class(is_debug=args.debug) as collector:
                cname = collector.__class__.__name__
                LOG.info(f'Starting {cname} collection.')

                exit_code = collector.run_collector()
                results = collector.metrics
                duplicate_keys = set(aggregated_metrics).intersection(results)

                if not duplicate_keys:
                    aggregated_metrics.update(results)
                    LOG.info(f'{cname} finished successfully.')
                else:
                    exit_code = 1
                    LOG.error(f'Collector key conflict on {duplicate_keys}.')
                    LOG.error(f'{cname} failed. Stopping all collection.')
                    break
    except Exception as err:
        exit_code = 1 if exit_code == 0 else exit_code
        LOG.error(err)

    return exit_code, aggregated_metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--debug",
                        action="store_true",
                        dest="debug",
                        default=False,
                        help="Enable debug output")

    parser.add_argument("--publish-results",
                        default="none",
                        type=str,
                        choices=PERFORMANCE_STORAGE_SERVICE_API.keys(),
                        help="Environment in which to store performance results")

    parser.add_argument("--publish-username",
                        type=str,
                        help="Performance Storage Service Username")

    parser.add_argument("--publish-password",
                        type=str,
                        help="Performance Storage Service password")

    args = parser.parse_args()

    if args.debug:
        LOG.setLevel(logging.DEBUG)

    # Get the BaseBinaryMetricsCollector subclasses imported from binary_metrics.binary_metrics_collectors
    # Effectively this adds each binary metric collector class into an array to be instantiated later.
    collectors = [obj for obj in BaseArtifactStatsCollector.__subclasses__()]
    exit_code, aggregated_metrics = collect_artifact_stats(collectors)

    if not exit_code:
        LOG.info(f'Artifact stats: {aggregated_metrics}')

        if args.publish_results != 'none':
            report_artifact_stats_result(args.publish_results, aggregated_metrics,
                                         args.publish_username, args.publish_password)

    logging.shutdown()
    sys.exit(exit_code)
