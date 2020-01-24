import csv
import numpy as np

import data_info
import query_info

from type import ArithmeticFeature


def get_global_data(filename):
    """Get the training data from the global model

    :param filename: the input data file
    :return: the list of global model data
    """

    if "txn" in filename:
        # Cannot handle the transaction manager data yet
        return []
    if "execution" in filename:
        # Special handle of the execution data
        return _execution_get_global_data(filename)

    return _default_get_global_data(filename)


def _execution_get_global_data(filename):
    # Get the global running data for the execution engine
    data_map = {}

    arithmetic_mode_index = data_info.arithmetic_feature_index[ArithmeticFeature.EXEC_MODE]

    data_list = []
    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        for line in reader:
            # The first element is always the query/pipeline identifier
            opunit_features = query_info.feature_map[line[0]]
            mode = int(line[1])
            for opunit_feature in opunit_features:
                opunit_feature[1].append(mode)
            line_data = list(map(int, line[2:]))
            data_list.append(GlobalData(opunit_features, np.array(line_data)))

    return data_list


def _default_get_global_data(filename):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    return []


class GlobalData:
    """
    The class that stores the data for the global model training
    """
    def __init__(self, opunit_features, y):
        """

        :param opunit: The list of opunits and their inputs for this event
        :param y: The runtime metrics
        """
        self.opunit_features = opunit_features
        self.y = y
