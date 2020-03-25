#!/usr/bin/env python3

import csv
import numpy as np
import pandas as pd
import os
import copy
import logging

from info import data_info
from util.io_util import write_csv_result

from type import OpUnit, ArithmeticFeature


def write_extended_data(output_path, symbol, index_value_list, data_map):
    # clear the content of the file
    open(output_path, 'w').close()

    write_csv_result(output_path, symbol, index_value_list)
    for key, value in data_map.items():
        write_csv_result(output_path, key, value)


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

    return _default_get_mini_runner_data(filename)


def _default_get_mini_runner_data(filename):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    x = df.iloc[:, :-data_info.METRICS_OUTPUT_NUM].values
    y = df.iloc[:, -data_info.MINI_MODEL_TARGET_NUM:].values

    logging.info("Loaded file: {}".format(OpUnit[file_name]))
    return [OpUnitData(OpUnit[file_name], x, y)]


def _execution_get_mini_runner_data(filename):
    # Get the mini runner data for the execution engine
    data_map = {}

    arithmetic_mode_index = data_info.ARITHMETIC_FEATURE_INDEX[ArithmeticFeature.EXEC_MODE]

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        for line in reader:
            # The first element is always the opunit name for the execution metrics
            opunit = OpUnit[line[0]]
            line_data = line[1:]
            if opunit not in data_map:
                data_map[opunit] = []
            # Do not record the compiled version of arithmetic operations since they'll all be optimized away
            if opunit not in data_info.ARITHMETIC_OPUNITS or line_data[arithmetic_mode_index] != '1':
                data_map[opunit].append(list(map(int, line_data)))

    data_list = []
    # Since the compiled arithmetics are always optimized away,
    # we treat the compiled arithmetics the same as the interpreted ones by copying the data
    for opunit, values in data_map.items():
        print(opunit)
        if opunit in data_info.ARITHMETIC_OPUNITS:
            compiled_values = copy.deepcopy(values)
            for v in compiled_values:
                v[arithmetic_mode_index] = 1
            values += compiled_values
        np_value = np.array(values)
        # print(np_value)
        x = np_value[:, :-data_info.METRICS_OUTPUT_NUM]
        y = np_value[:, -data_info.MINI_MODEL_TARGET_NUM:]
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
