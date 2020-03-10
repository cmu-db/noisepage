#!/usr/bin/env python3

import csv
import numpy as np
import pandas as pd
import os
import copy
import logging

import data_info
from io_util import write_csv_result

from type import OpUnit, ArithmeticFeature


def write_extended_data(output_path, symbol, index_value_list, data_map):
    # clear the content of the file
    open(output_path, 'w').close()

    write_csv_result(output_path, symbol, index_value_list)
    for key, value in data_map.items():
        write_csv_result(output_path, key, value)


def get_data_list(path):
    """Pre-process the raw data and get the list of Data object for each operation in the input file

    :param path: for the data
    :return: list of data objects
    """
    targets = ['memory', 'time']
    df = pd.read_csv(path, header=None)
    feature_n = df.shape[1] - 2
    feature_idx = list(range(feature_n))

    data_list = []
    for i, target in enumerate(targets):
        data = df.iloc[:, feature_idx + [feature_n + i]]
        col_n = data.shape[1]

        symbol_grouped_data = data.groupby(0)

        symbols = symbol_grouped_data.groups.keys()

        for symbol in symbols:
            # print(symbol)
            # first get the training data for each operation (symbol)
            symbol_group = symbol_grouped_data.get_group(symbol)
            # Then aggregate the labels for the same feature
            feature_grouped_data = symbol_group.groupby(list(range(1, col_n - 1)), as_index=False)
            # print(feature_grouped_data.groups.keys())
            data = feature_grouped_data.mean().to_numpy()
            # print(data)
            data_list.append(OldData(symbol, target, data))

    return data_list


def get_concurrent_data_list(path):
    '''
    Get the data for the concurrent experiments
    '''
    df = pd.read_csv(path, header=None)
    feature_idxes = []
    feature_idxes.append([0, 9, 13, -3])
    feature_idxes.append(list(range(4)) + list(range(9, 15)))
    feature_idxes.append(list(range(0, 9)) + [-4, -3])
    feature_idxes.append(list(range(0, 15)))
    # feature_idxes.append([0, 4, 8, 9])
    feature_idxes.append(list(range(0, 9)) + [9])
    feature_idxes.append(list(range(0, 9)) + [10])
    feature_idxes.append(list(range(0, 9)) + [11])
    feature_idxes.append(list(range(0, 9)) + [12])
    symbols = ['cpufeature', 'counterfeature', 'rawfeature', 'rawandcounterfeature', 'rawfeature', 'rawfeature',
               'rawfeature', 'rawfeature']
    targets = ['ratio', 'ratio', 'ratio', 'ratio', 'cpu', 'ref', 'miss', 'instr']
    data_list = []
    for i, feature_idx in enumerate(feature_idxes):
        data = df.iloc[:, feature_idx].values
        data_list.append(OldData(symbols[i], targets[i], data))

    return data_list


def transform_data_list(data_list):
    '''
    Transform a list of Data object to normalize them according to a given dimension
    '''
    for d in data_list:
        if 'probe' in d.symbol or 'memory' in d.target:
            # divide the row numbers
            transformed_data = d.get_label_transformed_data(0, np.divide)
            # then times the cardinality
            transformed_data = transformed_data.get_label_transformed_data(2, lambda x, y: x / y * 100)
        else:
            transformed_data = d.get_label_transformed_data(0, np.divide)

        print(d.symbol)

        index_value_list, data_map = transformed_data.extend_from_one_feature(0)

        output_path = "./{}_{}_extended.csv".format(d.symbol, d.target)

        write_extended_data(output_path, d.symbol, index_value_list, data_map)


def get_mini_runner_data(filename):
    """Get the training data from the mini runner

    :param filename: the input data file
    :return: the list of Data for execution operating units
    """

    if "txn" in filename:
        # Cannot handle the transaction manager data yet
        return []
    if "execution" in filename:
        # Special handle of the execution data
        return _execution_get_mini_runner_data(filename)

    return []

    return _default_get_mini_runner_data(filename)


def _default_get_mini_runner_data(filename):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    x = df.iloc[:, :-data_info.metrics_output_num].values
    y = df.iloc[:, -data_info.mini_model_target_num:].values

    logging.info("Loaded file: {}".format(OpUnit[file_name]))
    return [OpUnitData(OpUnit[file_name], x, y)]


def _execution_get_mini_runner_data(filename):
    # Get the mini runner data for the execution engine
    data_map = {}

    arithmetic_mode_index = data_info.arithmetic_feature_index[ArithmeticFeature.EXEC_MODE]

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        for line in reader:
            # The first element is always the opunit name for the execution metrics
            opunit = OpUnit[line[0]]
            line_data = line[1:]
            if not opunit in data_map:
                data_map[opunit] = []
            # Do not record the compiled version of arithmetic operations since they'll all be optimized away
            if opunit not in data_info.arithmetic_opunits or line_data[arithmetic_mode_index] != '1':
                data_map[opunit].append(list(map(int, line_data)))

    data_list = []
    # Since the compiled arithmetics are always optimized away,
    # we treat the compiled arithmetics the same as the interpreted ones by copying the data
    for opunit, values in data_map.items():
        print(opunit)
        if opunit in data_info.arithmetic_opunits:
            compiled_values = copy.deepcopy(values)
            for v in compiled_values:
                v[arithmetic_mode_index] = 1
            values += compiled_values
        np_value = np.array(values)
        # print(np_value)
        x = np_value[:, :-data_info.metrics_output_num]
        y = np_value[:, -data_info.mini_model_target_num:]
        data_list.append(OpUnitData(opunit, x, y))

    return data_list


class OpUnitData:
    """
    The class that stores data and provides basic functions to manipulate the training data for the operating unit
    """

    def __init__(self, opunit, x, y):
        """

        :param opunit: The opunit that the data is related to
        :param x: The input feature
        :param y: The outputs
        """
        self.opunit = opunit
        self.x = x
        self.y = y


class OldData:
    """The class that stores data and provides basic functions to manipulate the data

    IMPORTANT: This assumes the label is the last attribute in each row of self.data
    """

    def __init__(self, symbol, target, data):
        self.symbol = symbol
        self.target = target
        self.data = data

    def extend_from_one_feature(self, idx):
        """Pick one dimension to extend the labels

        IMPORTANT: This assumes that the data has same amount of labels when fixing all the other feature dimensions
        :param idx: the dimension to extend
        :return:
            - A list of all the different elements from that dimension - first output
            - A dictionary that maps the remaining features to the list of values - second output
        """

        df = pd.DataFrame(self.data)

        group_cols = []
        for i in range(0, df.shape[1] - 1):
            if i != idx:
                group_cols.append(i)

        grouped_data = df.groupby(group_cols)

        data_map = {}
        for key in grouped_data.groups.keys():
            data = grouped_data.get_group(key)

            data.sort_values([idx])

            index_value_list = data.iloc[:, idx]
            data_map[key] = data.iloc[:, -1].values

        return index_value_list, data_map

    def get_label_transformed_data(self, idx, func):
        """Apply a label transformation according to given index and function

        :param idx: the source column index for the transformation
        :param fuc: the transformation function
        :return: the new Data with the transformed label
        """

        new_data = np.copy(self.data)
        new_label = func(self.data[:, -1], self.data[:, idx])
        new_data[:, -1] = new_label

        return OldData(self.symbol, self.target, new_data)


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    input_path = "./aggregate.csv"
    # input_path = "./scan_interp.csv"

    get_data_list(input_path)
