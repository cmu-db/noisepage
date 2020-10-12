#!/usr/bin/python3

import os
import platform
import re

from reporting.parsers.oltpbench.config_parser import parse_config_file
from reporting.parsers.oltpbench.summary_parser import parse_summary_file
from reporting.parsers.oltpbench.res_parser import parse_res_file
from util.constants import DIR_UTIL
from reporting.constants import UNKNOWN_RESULT


def parse_oltpbench_data(results_dir):
    env_metadata = parse_jenkins_env_vars()
    files_metadata, timestamp, type, parameters, metrics = parse_oltpbench_files(results_dir)
    metadata = {**env_metadata, **files_metadata}
    return metadata, timestamp, type, parameters, metrics


def parse_microbenchmark_data(artifact_processor_comparison):
    env_metadata = parse_jenkins_env_vars()
    metadata = {**env_metadata, **parse_db_metadata()}
    test_suite, test_name, metrics = parse_microbenchmark_comparison(artifact_processor_comparison)
    return metadata, test_suite, test_name, metrics


def parse_jenkins_env_vars():
    """ get some metadata from the environment values that Jenkins has 
    populated """
    jenkins_job_id = os.environ['BUILD_ID']
    git_branch = os.environ['GIT_BRANCH']
    commit_id = os.environ['GIT_COMMIT']
    os_version =' '.join(platform.linux_distribution())
    os_cpu_number= os.cpu_count()
    #TODO find a way to get the socket number of 
    os_cpu_socket = 'true' 
    metadata = {
        'jenkins': {
            'jenkins_job_id': jenkins_job_id
        },
        'github': {
            'git_branch': git_branch,
            'git_commit_id': commit_id
        },
        'environment':{
            'os_version': os_version,
            'cpu_number': os_cpu_number,
            'cpu_socket': os_cpu_socket
        }
    }
    return metadata

def parse_oltpbench_files(results_dir):
    """
    Parse information from the config and summary files

    Args:
        results_dir (str): The location of directory where the oltpbench results are stored.
        
    Returns:
        metadata (dict): An object containing metadata information.
        timestamp (int): The timestamp when the benchmark was created in milliseconds.
        type (str): The type of OLTPBench test it was (tatp, noop, etc.)
        parameters (dict): Information about the parameters with which the test was run.
        metrics (dict): The summary measurements that were gathered from the test.
    """
    config_parameters = parse_config_file(results_dir+'/oltpbench.expconfig')
    metadata, timestamp, type, summary_parameters, metrics  = parse_summary_file(results_dir+'/oltpbench.summary')
    metrics['incremental_metrics']=parse_res_file(results_dir+'/oltpbench.res')
    parameters = {**summary_parameters,**config_parameters}
    return metadata, timestamp, type, parameters, metrics

def parse_microbenchmark_comparison(artifact_processor_comparison):
    metrics_fields =['throughput','stdev_throughput','tolerance','status','iterations','ref_throughput', 'num_results']
    test_suite, test_name = artifact_processor_comparison.get('suite'), artifact_processor_comparison.get('test')
    return test_suite, test_name, {key: value for key, value in artifact_processor_comparison.items() if key in metrics_fields}

def parse_db_metadata():
    regex = r"NOISEPAGE_VERSION[=\s].*(\d.\d.\d)"
    with open(DIR_UTIL+'../../src/include/common/version.h') as version_file:
        match = re.search(regex, version_file)
        return {
            'noisepage':{
                'db_version':  match.group(1) if match.group(1) else UNKNOWN_RESULT
            }
        }