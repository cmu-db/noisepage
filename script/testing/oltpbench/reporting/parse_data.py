#!/usr/bin/python3

import os
import distro
from oltpbench.reporting.parsers.config_parser import parse_config_file
from oltpbench.reporting.parsers.summary_parser import parse_summary_file
from oltpbench.reporting.parsers.res_parser import parse_res_file

def parse_data(results_dir):
    env_metadata = parse_jenkins_env_vars()
    files_metadata, timestamp, type, parameters, metrics = parse_files(results_dir)
    metadata = {**env_metadata, **files_metadata}
    return metadata, timestamp, type, parameters, metrics


def parse_jenkins_env_vars():
    jenkins_job_id = os.environ['BUILD_ID']
    git_branch = os.environ['GIT_BRANCH']
    commit_id = os.environ['GIT_COMMIT']
    os_version =' '.join(distro.linux_distribution())
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

def parse_files(results_dir):
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
