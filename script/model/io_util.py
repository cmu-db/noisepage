import csv


def write_result(path, label, data):
    """Write result data in csv format

    :param path: write destination
    :param label: the label (first column) to write
    :param data: the rest columns
    :return:
    """
    with open(path, "a") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([label] + list(data))