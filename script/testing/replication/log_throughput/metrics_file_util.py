import os
import shutil

from .constants import RESULTS_DIR


def get_csv_file_path(file_name):
    return os.path.join(os.getcwd(), file_name)


# TODO add timeout
def delete_metrics_file(file_name):
    path = get_csv_file_path(file_name)
    # It takes a little while for the metrics file to be created
    while not os.path.exists(path):
        pass
    os.remove(path)


def get_results_dir():
    return os.path.join(os.getcwd(), RESULTS_DIR)


def delete_results_dir():
    path = get_results_dir()
    if os.path.exists(path):
        shutil.rmtree(path)


def create_results_dir():
    delete_results_dir()
    os.makedirs(get_results_dir())


def move_metrics_file(file_name):
    path = get_csv_file_path(file_name)
    os.replace(path, os.path.join(get_results_dir(), file_name))
