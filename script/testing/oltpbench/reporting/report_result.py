#!/usr/bin/python3

import requests
from oltpbench.reporting.parse_data import parse_data, add_mem_metrics
from oltpbench.constants import PERFORMANCE_STORAGE_SERVICE_API


def report(api_url,
           server_data,
           results_dir,
           username,
           password,
           mem_metrics,
           query_mode='simple'):
    metadata, timestamp, benchmark_type, parameters, metrics = parse_data(
        results_dir)
    add_mem_metrics(metrics, mem_metrics)
    parameters['query_mode'] = query_mode
    parameters['max_connection_threads'] = server_data[
        'max_connection_threads']
    metadata['environment']['wal_device'] = server_data['wal_device']
    results = {
        'metadata': metadata,
        'timestamp': timestamp,
        'type': benchmark_type,
        'parameters': parameters,
        'metrics': metrics
    }
    send_results(api_url, username, password, results)


def send_results(api_url, username, password, results):
    result = requests.post(api_url + '/oltpbench/',
                           json=results,
                           auth=(username, password))
    result.raise_for_status()