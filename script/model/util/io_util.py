import csv


def write_csv_result(path, label, data):
    """Write result data in csv format

    :param path: write destination
    :param label: the label (first column) to write
    :param data: the rest columns
    :return:
    """
    with open(path, "a") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([label] + list(data))


def create_csv_file(path, header):
    """Create a new csv file with header (replace any existing one)

    :param path: write destination
    :param header: the list for the headers in the file
    """

    open(path, 'w').close()
    if header is not None:
        write_csv_result(path, header[0], header[1:])
