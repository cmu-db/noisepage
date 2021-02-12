import os
import re
from decimal import Decimal

import requests
import distro

from ...util.constants import LOG
from ..constants import UNKNOWN_RESULT
from .oltpbench.config_parser import parse_config_file
from .oltpbench.res_parser import parse_res_file
from .oltpbench.summary_parser import parse_summary_file


def parse_oltpbench_data(results_dir):
    """
    Collect the information needed to send to the performance storage service
    from the files produced by OLTPBench.

    Args
    -----
    results_dir : str
        The directory where the OLTPBench results were stored
    Returns
    --------
    metadata : dict
        The metadata of the OLTPBench test
    timestamp : int
        When the test was run in milliseconds
    type : str
        The benchmark type (i.e. tpcc)
    parameters : dict
        The parameters that were used to run the test
    metrics : dict
        The metrics gathered from the result of the test
    """
    env_metadata = _parse_jenkins_env_vars()
    files_metadata, timestamp, benchmark_type, parameters, metrics = parse_oltpbench_files(
        results_dir)
    metadata = {**env_metadata, **files_metadata}
    return metadata, timestamp, benchmark_type, parameters, metrics


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
    metadata = parse_standard_metadata()
    test_suite, test_name, metrics = parse_microbenchmark_comparison(
        artifact_processor_comparison)
    return metadata, test_suite, test_name, metrics


def parse_standard_metadata():
    """
    Gather the standard metadata information from Jenkins and the DBMS.

    Returns
    -------
    The metadata obtained from Jenkins and the DBMS.

    Warnings
    --------
    Underlying implementation is hacky right now.
    """
    return {**_parse_jenkins_env_vars(), **_parse_db_metadata()}


def _parse_jenkins_env_vars():
    """
    Parse environment variables from Jenkins and the OS.

    Returns
    -------
    metadata : dict
        Metadata about the Jenkins environment.
        WARNING: Note that cpu_socket is a completely garbage value.
        TODO(WAN): I'd remove cpu_socket except I'm afraid of breakages.
    """
    # TODO find a way to get the socket number of
    os_cpu_socket = 'true'

    metadata = {
        'jenkins': {
            'jenkins_job_id': os.environ['BUILD_ID'],
        },
        'github': {
            'git_branch': os.environ['GIT_BRANCH'],
            'git_commit_id': _get_git_commit_id(os.environ['GIT_BRANCH']),
        },
        'environment': {
            'os_version': ' '.join(distro.linux_distribution()),
            'cpu_number': os.cpu_count(),
            'cpu_socket': os_cpu_socket
        }
    }
    return metadata

def _get_git_commit_id(git_branch):
    """
    Get the commit hash.
    
    Parameters
    ----------
    git_branch : str
        The branch that the code is from. For pull-requests this will be 
        PR-####.

    Returns
    -------
    str
        The commit hash for the latest commit on the branch.
    """
    pr_number_match = re.search('PR-\d+', git_branch)
    if pr_number_match:
        # Get the hash from the last commit to the PR.
        try:
            pr_number = int(pr_number_match.group(1))
            commits = _get_git_pr_commits(pr_number)
            latest_commit = commits[-1]
            return latest_commit.get('sha')
        except:
            # If there are problems with API call just use the env variable.
            pass
    # Non-PRs won't have a merge commit so this value is valid.
    return os.environ['GIT_COMMIT']

def _get_git_pr_commits(pr_number):
    """
    Get all the commits for a PR.

    Parameters
    ----------
    pr_number : int
        The number of the PR. For PR-1234 the pr_number would be 1234.

    Returns
    -------
    list of dict
        A list of commits as dicts.
    """
    headers = {}
    if os.environ.get('GITHUB_TOKEN'):
        # If there is a token use it. Otherwise it will use unauthenticated
        # call that has a low rate limit.
        headers['Authorization'] = f'token {os.environ.get("GITHUB_TOKEN")}'

    response = requests.get(f'https://api.github.com/repos/cmu-db/noisepage/pulls/{pr_number}/commits',
                            headers=headers)
    response.raise_for_status()
    return response.json()

def parse_oltpbench_files(results_dir):
    """
    Parse information from the config and summary files generated by OLTPBench.

    Parameters
    ----------
    results_dir : str
        The directory where OLTPBench results are stored.

    Returns
    -------
    metadata : dict
        An object containing metadata information.
    timestamp : int
        The timestamp when the benchmark was created, in milliseconds.
        TODO(WAN): wtf is this?
    benchmark_type : str
        The benchmark that was run (e.g., tatp, noop).
    parameters : dict
        Information about the parameters with which the test was run.
    metrics : dict
        The summary measurements that were gathered from the test.
    """
    config_parameters = parse_config_file(results_dir + '/oltpbench.expconfig')
    metadata, timestamp, benchmark_type, summary_parameters, metrics = parse_summary_file(
        results_dir + '/oltpbench.summary')
    metrics['incremental_metrics'] = parse_res_file(results_dir +
                                                    '/oltpbench.res')
    parameters = {**summary_parameters, **config_parameters}
    return metadata, timestamp, benchmark_type, parameters, metrics


def parse_microbenchmark_comparison(artifact_processor_comparison):
    """ Extract the relevant information from the artifact_processor_comparison 
    and parse out the test name and suite"""
    metrics_fields = [
        'throughput', 'stdev_throughput', 'tolerance', 'status', 'iterations',
        'ref_throughput', 'num_results'
    ]
    test_suite, test_name = artifact_processor_comparison.get(
        'suite'), artifact_processor_comparison.get('test')

    metrics = {}
    for key, value in artifact_processor_comparison.items():
        if key in metrics_fields:
            metrics[key] = round(value, 15) if isinstance(
                value, (float, Decimal)) else value
    return test_suite, test_name, metrics


def _parse_db_metadata():
    """
    Parse metadata from the DBMS.

    Returns
    -------
    metadata : dict
        A dictionary containing metadata about the database.

    Warnings
    --------
    Giant hack that parses a hardcoded constant NOISEPAGE_VERSION
    in src/include/common/version.h.

    If the hack is unsuccessful, it defaults to UNKNOWN_RESULT.
    """
    regex = r"NOISEPAGE_VERSION[=\s].*(\d.\d.\d)"
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    # TODO(WAN): Don't do this. We support SELECT VERSION(), do that instead.
    version_file_relative = '../../../../src/include/common/version.h'
    version_file = os.path.join(curr_dir, version_file_relative)
    db_metadata = {'noisepage': {'db_version': UNKNOWN_RESULT}}
    try:
        with open(version_file) as f:
            match = re.search(regex, f.read())
            db_metadata['noisepage']['db_version'] = match.group(1)
    except Exception as err:
        LOG.error(err)

    return db_metadata
