#!/usr/bin/python3
import requests
from datetime import datetime

from reporting.parsers.parse_data import parse_oltpbench_data, parse_microbenchmark_data, parse_standard_metadata
from reporting.parsers.microbenchmark.config_parser import parse_parameters, parse_wal_device
from reporting.utils import add_mem_metrics
from util.constants import PERFORMANCE_STORAGE_SERVICE_API
from util.constants import LOG


def report_oltpbench_result(env, server_data, results_dir, username, password, mem_metrics, query_mode='simple'):
    """ Parse and format the data from server_data and the results_dir into a
    JSON body and send those results to the performance storage service"""
    LOG.debug("parsing OLTPBench results and assembling request body.")
    metadata, timestamp, benchmark_type, parameters, metrics = parse_oltpbench_data(
        results_dir)
    add_mem_metrics(metrics, mem_metrics)
    parameters['query_mode'] = query_mode
    parameters['max_connection_threads'] = server_data.get(
        'max_connection_threads')
    metadata['environment']['wal_device'] = server_data.get('wal_device')
    result = {
        'metadata': metadata,
        'timestamp': timestamp,
        'type': benchmark_type,
        'parameters': parameters,
        'metrics': metrics
    }

    send_result(env, '/oltpbench/', username, password, result)


def report_microbenchmark_result(env, timestamp, config,
                                 artifact_processor_comparison):
    """ Parse and format the data from the microbenchmark tests into a JSON
    body and send those to the performance storage service. """
    LOG.debug("parsing OLTPBench results and assembling request body.")
    metadata, test_suite, test_name, metrics = parse_microbenchmark_data(
        artifact_processor_comparison)
    parameters = parse_parameters(config)
    metadata['environment']['wal_device'] = parse_wal_device(config)

    result = {
        'metadata': metadata,
        'timestamp':
        int(timestamp.timestamp() * 1000),  # convert to milliseconds
        'test_suite': test_suite,
        'test_name': test_name,
        'parameters': parameters,
        'metrics': metrics
    }
    send_result(env, '/microbenchmark/', config.publish_results_username,
                config.publish_results_password, result)


def report_artifact_stats_result(env, metrics, username, password):
    """ Parse and format the data from the artifact stats into a JSON body and
    send those to the performance storage service. """
    result = {
        'metadata': parse_standard_metadata(),
        'timestamp': int(datetime.now().timestamp() * 1000),  # convert to milliseconds
        'metrics': metrics
    }
    send_result(env, '/artifact-stats/', username, password, result)


def send_result(env, path, username, password, result):
    """ Send the results to the performance storage service. If the service
    responds with an error code this will raise an error. """
    LOG.debug("Sending request to {PATH}".format(PATH=path))
    base_url = get_base_url(env)
    try:
        result = requests.post(base_url + path,
                               json=result,
                               auth=(username, password))
        result.raise_for_status()
    except Exception as err:
        LOG.error(err.response.text)
        raise


def get_base_url(environment):
    """ Determind the performance storage service URL to
    use based on the environemnt string. """
    return PERFORMANCE_STORAGE_SERVICE_API.get(environment)
