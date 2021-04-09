import os
import shutil

from .constants import RESULTS_DIR


def get_csv_file_path(file_name: str) -> str:
    """
    Get absolute path to csv metrics file

    :param file_name Name of metrics file
    """
    return os.path.join(os.getcwd(), file_name)


# TODO add timeout
def delete_metrics_file(file_name: str):
    """
    Deletes a metrics file

    :param file_name Name of metrics file
    """
    path = get_csv_file_path(file_name)
    # It takes a little while for the metrics file to be created
    while not os.path.exists(path):
        pass
    os.remove(path)


def get_results_dir() -> str:
    """
    Get absolute path to results directory
    """
    return os.path.join(os.getcwd(), RESULTS_DIR)


def delete_results_dir():
    """
    Deletes results directory
    """
    path = get_results_dir()
    if os.path.exists(path):
        shutil.rmtree(path)


def create_results_dir():
    """
    Creates results directory
    """
    delete_results_dir()
    os.makedirs(get_results_dir())


def move_metrics_file_to_results_dir(file_name: str):
    """
    Moves metric csv file to results directory
    """
    path = get_csv_file_path(file_name)
    os.replace(path, os.path.join(get_results_dir(), file_name))
