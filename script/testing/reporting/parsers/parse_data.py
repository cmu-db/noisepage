#!/usr/bin/python3

import os
import distro
import re

from reporting.parsers.oltpbench.config_parser import parse_config_file
from reporting.parsers.oltpbench.summary_parser import parse_summary_file
from reporting.parsers.oltpbench.res_parser import parse_res_file
from util.constants import DIR_UTIL
from reporting.constants import UNKNOWN_RESULT


def parse_oltpbench_data(results_dir):
    """ 
    Collect the information needed to send to the performance storage service
    from the files produced by OLTPBench

    Args:
        results_dir (str): The directory where the OLTPBench results were stored

    Returns: 
        metadata (dict): The metadata of the OLTPBench test
        timestamp (int): When the test was run in milliseconds
        type (str): The benchmark type (i.e. tpcc)
        parameters (dict): The parameters that were used to run the test
        metrics (dict): The metrics gathered from the result of the test
    """
    env_metadata = parse_jenkins_env_vars()
    files_metadata, timestamp, type, parameters, metrics = parse_oltpbench_files(
        results_dir)
    metadata = {**env_metadata, **files_metadata}
    return metadata, timestamp, type, parameters, metrics


def parse_microbenchmark_data(artifact_processor_comparison):
    """ 
    Collect the information needed to send to the performance storage service
    from the files produced by the microbenchmark

    Args:
        artifact_processor_comparison (dict): The comparison object generated 
                                                by the artifact processor

    Returns: 
        metadata (dict): The metadata of the microbenchmark test
        test_suite (str): The name of the test suite
        test_name (str): The name of the specific benchmark test
        metrics (dict): The metrics gathered from the result of the test
    """
    env_metadata = parse_jenkins_env_vars()
    metadata = {**env_metadata, **parse_db_metadata()}
    test_suite, test_name, metrics = parse_microbenchmark_comparison(
        artifact_processor_comparison)
    return metadata, test_suite, test_name, metrics


def parse_jenkins_env_vars():
    """ get some metadata from the environment values that Jenkins has 
    populated and from the operating system"""
    jenkins_job_id = os.environ['BUILD_ID']
    git_branch = os.environ['GIT_BRANCH']
    commit_id = os.environ['GIT_COMMIT']
    os_version = ' '.join(distro.linux_distribution())
    os_cpu_number = os.cpu_count()
    # TODO find a way to get the socket number of
    os_cpu_socket = 'true'
    metadata = {
        'jenkins': {
            'jenkins_job_id': jenkins_job_id
        },
        'github': {
            'git_branch': git_branch,
            'git_commit_id': commit_id
        },
        'environment': {
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
    config_parameters = parse_config_file(results_dir + '/oltpbench.expconfig')
    metadata, timestamp, type, summary_parameters, metrics = parse_summary_file(
        results_dir + '/oltpbench.summary')
    metrics['incremental_metrics'] = parse_res_file(
        results_dir + '/oltpbench.res')
    parameters = {**summary_parameters, **config_parameters}
    return metadata, timestamp, type, parameters, metrics


def parse_microbenchmark_comparison(artifact_processor_comparison):
    """ Extract the relevant information from the artifact_processor_comparison 
    and parse out the test name and suite"""
    metrics_fields = ['throughput', 'stdev_throughput', 'tolerance',
                      'status', 'iterations', 'ref_throughput', 'num_results']
    test_suite, test_name = artifact_processor_comparison.get(
        'suite'), artifact_processor_comparison.get('test')
    return test_suite, test_name, {key: value for key, value in artifact_processor_comparison.items() if key in metrics_fields}


def parse_db_metadata():
    """ Lookup the DB version from the version.h file. This is error prone
    due to it being reliant on file location and no tests that will fail
    if the version.h moves and this is not updated. We use a fallback of
    unknown result so the tests won't fail if this happens"""
    regex = r"NOISEPAGE_VERSION[=\s].*(\d.\d.\d)"
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    db_metadata = {
        'noisepage': {
            'db_version': UNKNOWN_RESULT
        }
    }
    try:
        with open(CURRENT_DIR + '/../../../../src/include/common/version.h') as version_file:
            match = re.search(regex, version_file.read())
            db_metadata['noisepage']['db_version'] = match.group(1)
    except Exception as err:
        LOG.error(err)

    return db_metadata
