#!/usr/bin/env python3
import os
import sys
import argparse
import logging

base_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, base_path)

from artifact_stats.base_artifact_stats_collector import BaseArtifactStatsCollector
from artifact_stats.collectors import *
from reporting.report_result import report_artifact_stats_result
from util.constants import PERFORMANCE_STORAGE_SERVICE_API, LOG


def collect_artifact_stats(collectors):
    """ Takes an array of collector classes, executes the collectors and 
    combines the result.
    Args:
        collectors - An array of BaseArtifactStatsCollector sub classes
    Returns:
        exit_code - (int)The exit code of the collection task
        metrics - (dict)The combined metrics from all the collectors
    """
    aggregated_metrics = {}
    exit_code = 0
    try:
        for collector in collectors:
            collector_instance = collector(is_debug=args.debug)
            LOG.info(f'Starting {collector_instance.__class__.__name__} collection')
            try:
                exit_code, results = run_collector(collector_instance)
                check_for_conflicting_metric_keys(aggregated_metrics, results)
                aggregated_metrics.update(results)
            except Exception as err:
                exit_code = 1 if exit_code == 0 else exit_code
                LOG.error(err)
                collector_instance.teardown()
            if exit_code:
                LOG.error(f'{collector_instance.__class__.__name__} failed. Stopping all artifact stats collection')
                break
            LOG.info(f'{collector_instance.__class__.__name__} finished successfully')
    except Exception as err:
        exit_code = 1 if exit_code == 0 else exit_code
        LOG.error(err)

    return exit_code, aggregated_metrics


def run_collector(collector_instance):
    """ Execute a collector. This includes setup, metric collection, and 
    teardown. 
    Args:
        collector_instance - An instance of a BaseArtifactStatsCollector subclass
    Returns:
        exit_code - (int)The exit code of the collection task
        metrics - (dict)The metrics collected during the collection task 
    """
    LOG.debug('Running setup...')
    collector_instance.setup()

    LOG.debug('Collecting metrics...')
    exit_code = collector_instance.run_collector()

    LOG.debug('Running teardown...')
    collector_instance.teardown()

    results = collector_instance.get_metrics()
    return exit_code, results


def check_for_conflicting_metric_keys(aggregated_metrics, collector_results):
    """ Check to see if another collector has already added a metric with the
    same key to the aggregated metrics. If there is a conflict an exception is
    raised """
    shared_keys = set(aggregated_metrics).intersection(collector_results)
    if shared_keys:
        raise KeyError(f'artifact stats collector key conflict on {shared_keys}')
    return


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
