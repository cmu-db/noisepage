#!/usr/bin/python3

import requests
from reporting.parsers.parse_data import parse_oltpbench_data
from reporting.parsers.microbenchmark.config_parser import parse_parameters, parse_wal_device
from oltpbench.constants import PERFORMANCE_STORAGE_SERVICE_API

def report_oltpbench_result(env, server_data, results_dir, username, password, query_mode='simple'):
    metadata, timestamp, type, parameters, metrics = parse_oltpbench_data(results_dir)
    parameters['query_mode'] = query_mode
    parameters['max_connection_threads'] = server_data['max_connection_threads']
    metadata['environment']['wal_device'] = server_data['wal_device']
    result = {
        'metadata': metadata,
        'timestamp': timestamp,
        'type': type,
        'parameters': parameters,
        'metrics': metrics
    }
    
    send_result(env, '/oltpbench/', username, password, result)

def report_microbenchmark_result(env, timestamp, config, artifact_processor_comparison):
    metadata, test_suite, test_name, metrics = parse_microbenchmark_data(artifact_processor_comparison)
    parameters = parse_parameters(config)
    metadata['environment']['wal_device'] = parse_wal_device(config)
    result = {
        'metadata': metadata,
        'timestamp': timestamp,
        'test_suite': test_suite,
        'test_name': test_name,
        'parameters': parameters,
        'metrics': metrics
    }
    
    send_result(env, '/microbenchmark/', config.publish_results_username, config.publish_results_password, result)

def send_result(env, path, username, password, result):
    base_url = get_base_url(env)
    result = requests.post(base_url + path, json=result, auth=(username,password))
    result.raise_for_status()

def get_base_url(environment):
    return PERFORMANCE_STORAGE_SERVICE_API.get(environment)