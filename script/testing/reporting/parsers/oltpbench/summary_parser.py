#!/usr/bin/python3

import json
from time import time

from reporting.utils import get_value_by_pattern
from reporting.constants import UNKNOWN_RESULT, LATENCY_ATTRIBUTE_MAPPING


def parse_summary_file(path):
    """
    Read data from file OLTPBench summary file.

    Args:
        path (str): The location of the summary file.

    Returns:
        metadata (dict): An object containing metadata information.
        timestamp (int): The timestamp when the benchmark was created in milliseconds.
        type (str): The type of OLTPBench test it was (tatp, noop, etc.)
        parameters (dict): Information about the parameters with which the test was run.
        metrics (dict): The summary measurements that were gathered from the test.
    """
    with open(path) as summary_file:
        summary = json.load(summary_file)
        metadata = parse_metadata(summary)
        timestamp = parse_timestamp(summary)
        type = parse_type(summary)
        parameters = parse_parameters(summary)
        metrics = parse_metrics(summary)
        return metadata, timestamp, type, parameters, metrics


def parse_metadata(summary):
    """
    Get the db_version from the summary file data 

    Args:
        summary (dict): The JSON of the summary file.

    Returns:
        metadata: metadata collected from the summary file

    """
    return {
        'noisepage': {
            'db_version': summary.get('DBMS Version', UNKNOWN_RESULT)
        }
    }


def parse_timestamp(summary):
    """ Get the timestamp in milliseconds from the summary file data """
    return int(get_value_by_pattern(summary, 'timestamp', str(time())))


def parse_type(summary):
    """ Get the benchmark type (i.e tpcc) from the summary file data """
    return summary.get('Benchmark Type', UNKNOWN_RESULT)


def parse_parameters(summary):
    """ Collect the OLTPBench test parameters from the summary file data """
    return {
        'scale_factor': summary.get('scalefactor', '-1.0'),
        'terminals': int(summary.get('terminals', -1))
    }


def parse_metrics(summary):
    """ Collect the OLTPBench results metrics from the summary file data """
    return {
        'throughput': get_value_by_pattern(summary, 'throughput', '-1.0'),
        'latency': parse_latency_data(summary.get('Latency Distribution', {}))
    }


def parse_latency_data(latency_dict):
    """
    Parses the latency data from the format it is stored in the summary
    file to the format we need for the API request.

    Args:
        latency_dict (dict): The "Latency Distribution" json object in the OLTPBench
        summary file.

    Returns:
        latency (dict): The latency dict required to send to the performance storage
        service.
    """
    latency = {}
    for key, pattern in LATENCY_ATTRIBUTE_MAPPING:
        value = get_value_by_pattern(latency_dict, pattern, None)
        latency[key] = float("{:.4}".format(value)) if value else value
    return latency
