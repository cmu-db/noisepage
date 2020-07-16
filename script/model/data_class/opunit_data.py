#!/usr/bin/env python3

import csv
import numpy as np
import pandas as pd
import os
import logging
import tqdm

from data_class import data_util
from info import data_info
from util import io_util

from type import OpUnit


def write_extended_data(output_path, symbol, index_value_list, data_map):
    # clear the content of the file
    open(output_path, 'w').close()

    io_util.write_csv_result(output_path, symbol, index_value_list)
    for key, value in data_map.items():
        io_util.write_csv_result(output_path, key, value)


def get_mini_runner_data(filename, model_results_path, model_map={}, predict_cache={}, subtract_style="manual"):
    """Get the training data from the mini runner

    :param filename: the input data file
    :param model_results_path: results log directory
    :param model_map: the map from OpUnit to the mini model
    :param predict_cache: cache for the mini model prediction
    :param subtract_style: subtraction style for eliminating units
    :return: the list of Data for execution operating units
    """

    if "txn" in filename:
        # Cannot handle the transaction manager data yet
        return []
    if "execution" in filename:
        # Handle the execution data
        return _execution_get_mini_runner_data(filename, model_map, predict_cache, subtract_style)
    if "gc" in filename or "log" in filename:
        # Handle of the gc or log data with interval-based conversion
        return _interval_get_mini_runner_data(filename, model_results_path)

    return []


def _interval_get_mini_runner_data(filename, model_results_path):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    x = df.iloc[:, :-data_info.METRICS_OUTPUT_NUM].values
    y = df.iloc[:, -data_info.MINI_MODEL_TARGET_NUM:].values
    start_times = df.iloc[:, data_info.TARGET_CSV_INDEX[data_info.Target.START_TIME]].values

    logging.info("Loaded file: {}".format(OpUnit[file_name.upper()]))

    # change the data based on the interval for the periodically invoked operating units
    prediction_path = "{}/{}_interval_converted_data.csv".format(model_results_path, file_name)
    io_util.create_csv_file(prediction_path, [""])

    interval = data_info.PERIODIC_OPUNIT_INTERVAL

    # Map from interval start time to the data in this interval
    interval_x_map = {}
    interval_y_map = {}
    n = x.shape[0]
    for i in tqdm.tqdm(list(range(n)), desc="Group data by interval"):
        rounded_time = data_util.round_to_interval(start_times[i], interval)
        if rounded_time not in interval_x_map:
            interval_x_map[rounded_time] = []
            interval_y_map[rounded_time] = []
        interval_x_map[rounded_time].append(x[i])
        interval_y_map[rounded_time].append(y[i])

    # Construct the new data
    x_list = []
    y_list = []
    for rounded_time in interval_x_map:
        # Sum the features
        x_new = np.sum(interval_x_map[rounded_time], axis=0)
        # Keep the interval parameter the same
        # TODO: currently the interval parameter is always the last. Change the hard-coding later
        x_new[-1] /= len(interval_x_map[rounded_time])
        x_list.append(x_new)
        # The prediction is the average behavior
        y_list.append(np.average(interval_y_map[rounded_time], axis=0))
        io_util.write_csv_result(prediction_path, rounded_time, np.concatenate((x_list[-1], y_list[-1])))

    return [OpUnitData(OpUnit[file_name.upper()], np.array(x_list), np.array(y_list))]

def _execution_get_mini_runner_data(filename, model_map, predict_cache, subtract_style):
    # Get the mini runner data for the execution engine
    data_map = {}
    execution_mode_index = data_info.RAW_EXECUTION_MODE_INDEX
    features_vector_index = data_info.RAW_FEATURES_VECTOR_INDEX

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        for line in reader:
            # drop query_id, pipeline_id, num_features, features_vector
            record = [d for i, d in enumerate(line) if i > features_vector_index]
            record.insert(data_info.EXECUTION_MODE_INDEX, line[execution_mode_index])
            data = list(map(data_util.convert_string_to_numeric, record))
            x_multiple = data[:data_info.RECORD_FEATURES_END]
            y_merged = np.array(data[-data_info.RECORD_METRICS_START:])

            # Get the opunits located within
            opunits = []
            features = line[features_vector_index].split(';')
            for idx, feature in enumerate(features):
                opunit = OpUnit[feature]
                x_loc = [v[idx] if type(v) == list else v for v in x_multiple]
                if opunit in model_map:
                    key = [opunit] + x_loc
                    if tuple(key) not in predict_cache or subtract_style == "model":
                        predict = model_map[opunit].predict(np.array(x_loc).reshape(1, -1))[0]
                        predict_cache[tuple(key)] = [predict] # only 1 prediction
                        y_merged = y_merged - predict
                    else:
                        predict = np.average(predict_cache[tuple(key)], axis=0)[0]
                        predict = np.clip(predict, 0, None)
                        y_merged = y_merged - predict

                    y_merged = np.clip(y_merged, 0, None)
                else:
                    opunits.append((opunit, x_loc))

            if len(opunits) > 1:
                raise Exception('Unmodelled OperatingUnits detected: {}'.format(opunits))

            # opunits[0][0] is the opunit
            # opunits[0][1] is input feature
            # y_merged should be post-subtraction
            if opunits[0][0] not in data_map:
                data_map[opunits[0][0]] = []
            data_map[opunits[0][0]].append(opunits[0][1] + list(y_merged))

    data_list = []
    for opunit, values in data_map.items():
        np_value = np.array(values)
        x = np_value[:, :data_info.RECORD_FEATURES_END]
        y = np_value[:, -data_info.RECORD_METRICS_START:]
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
