import os
import time
from typing import Set

from ...util.constants import LOG
from .constants import RESULTS_DIR

"""
This file exposes a bunch of helper methods for interacting with metric csv files
"""


def get_csv_file_path(file_name: str) -> str:
    """
    Get absolute path to csv metrics file

    Parameters
    ----------
    file_name
        Name of metrics file

    Returns
    -------
    file_path
        Full path to csv file
    """
    return os.path.join(os.getcwd(), file_name)


def delete_metrics_files(file_names: Set[str]):
    """
    Deletes metrics files

    Parameters
    ----------
    file_names
        Name of all metrics files to delete
    """
    for file_name in file_names:
        delete_metrics_file(file_name)


def delete_metrics_file(file_name: str):
    """
    Deletes a metrics file

    Parameters
    ----------
    file_name
        Name of metrics file
    """
    timeout = 1
    path = get_csv_file_path(file_name)
    # It takes a little while for the metrics file to be created
    start = time.time()
    while not os.path.exists(path):
        if time.time() - start > timeout:
            LOG.warn(f"File {file_name} not found")
            return
    os.remove(path)


def get_results_dir() -> str:
    """
    Get absolute path to results directory

    Returns
    -------
    results_dir
        Full path to results directory
    """
    return os.path.join(os.getcwd(), RESULTS_DIR)


def create_results_dir():
    """
    Creates results directory
    """
    if not os.path.exists(get_results_dir()):
        os.makedirs(get_results_dir())


def move_metrics_file_to_results_dir(file_name: str, output_file: str):
    """
    Moves metric csv file to results directory

    Parameters
    ----------
    file_name
        Name of file containing metrics
    output_file
        Name of destination to move file to
    """
    path = get_csv_file_path(file_name)
    os.replace(path, os.path.join(get_results_dir(), output_file))
