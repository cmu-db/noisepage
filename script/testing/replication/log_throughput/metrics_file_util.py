import os
import time

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
    timeout = 1
    path = get_csv_file_path(file_name)
    # It takes a little while for the metrics file to be created
    start = time.time()
    while not os.path.exists(path):
        if time.time() - start > timeout:
            print(f"File {file_name} not found")
            return
    os.remove(path)


def get_results_dir() -> str:
    """
    Get absolute path to results directory
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

    :param file_name Name of file containing metrics
    :param output_file Name of destination to move file to
    """
    path = get_csv_file_path(file_name)
    os.replace(path, os.path.join(get_results_dir(), output_file))
