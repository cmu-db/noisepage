#!/usr/bin/python3

import requests
from reporting.parsers.parse_data import parse_oltpbench_data
from oltpbench.constants import PERFORMANCE_STORAGE_SERVICE_API

def report_oltpbench_result(env, server_data, results_dir, username, password, query_mode='simple'):
    metadata, timestamp, type, parameters, metrics = parse_oltpbench_data(results_dir)
    parameters['query_mode'] = query_mode
    parameters['max_connection_threads'] = server_data['max_connection_threads']
    metadata['environment']['wal_device'] = server_data['wal_device']
    results = {
        'metadata': metadata,
        'timestamp': timestamp,
        'type': type,
        'parameters': parameters,
        'metrics': metrics
    }
    
    send_result(env, '/oltpbench/', username, password, results)

def send_result(env, path, username, password, results):
    base_url = get_base_url(env)
    result = requests.post(base_url + path, json=results, auth=(username,password))
    result.raise_for_status()

def get_base_url(environment):
    return PERFORMANCE_STORAGE_SERVICE_API.get(environment)