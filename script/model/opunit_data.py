#!/usr/bin/env python3

import csv
import numpy as np
import pandas as pd
import os
import copy
import logging

import data_info
from util.io_util import write_csv_result

from type import OpUnit, ArithmeticFeature

def _convert_string_to_numeric(value):
    if ';' in value:
        return list(map(_convert_string_to_numeric, value.split(';')))

    try:
        return int(value)
    except:
        # Scientific notation
        return int(float(value))

def get_mini_runner_data(filename):
    """Get the training data from the mini runner

    :param filename: the input data file
    :return: the list of Data for execution operating units
    """
    return _execution_get_mini_runner_data(filename);


def _execution_get_mini_runner_data(filename):
    # Get the mini runner data for the execution engine
    data_map = {}
    execution_mode_index = data_info.RAW_EXECUTION_MODE_INDEX
    features_vector_index = data_info.RAW_FEATURES_VECTOR_INDEX
    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        for line in reader:
            # The first element is always the opunit name for the execution metrics
            opunits = []
            features = line[features_vector_index].split(';')
            for feature in features:
                opunit = OpUnit[feature]
                if opunit not in data_info.ARITHMETIC_OPUNITS or line[execution_mode_index] != '1':
                    opunits.append(OpUnit[feature])

            if not opunits:
                continue

            # sort in increasing order of OpUnit
            opunit_tuple = tuple(opunits)

            # drop query_id, pipeline_id, num_features, features_vector
            record = [d for i,d in enumerate(line) if i > features_vector_index]
            record.insert(data_info.EXECUTION_MODE_INDEX, line[execution_mode_index])

            if opunit_tuple not in data_map:
                data_map[opunit_tuple] = []
            data_map[opunit_tuple].append(list(map(_convert_string_to_numeric, record)))

    data_list = []
    for opunits, values in data_map.items():
        # Since the compiled arithmetics are always optimized away,
        # we treat the compiled arithmetics the same as the interpreted ones by copying the data
        if len(opunits) == 1 and opunits[0] in data_info.ARITHMETIC_OPUNITS:
            compiled_values = copy.deepcopy(values)
            for v in compiled_values:
                v[data_info.EXECUTION_MODE_INDEX] = 1 # mark as compiled
            values += compiled_values

        np_value = np.array(values)
        x = np_value[:, :data_info.RECORD_FEATURES_END]
        y = np_value[:, -data_info.RECORD_METRICS_START:]
        data_list.append(OpUnitData(opunits, x, y))

    return data_list


class OpUnitData:
    """
    The class that stores data and provides basic functions to manipulate the training data for the operating unit
    """

    def __init__(self, opunits, x, y):
        """
        :param opunits: The opunits that the data is related to
        :param x: The input feature
        :param y: The outputs
        """
        self.opunits = opunits
        self.x = x
        self.y = y
