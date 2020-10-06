import csv
import numpy as np
import copy
import tqdm
import pandas as pd
import os
import logging

from data_class import data_util
from data_class import tpcc_fixer
from info import data_info, query_info, query_info_1G, query_info_10G
import global_model_config

from type import Target, ConcurrentCountingMode, OpUnit


def get_grouped_op_unit_data(filename, warmup_period, tpcc_hack, ee_sample_interval, txn_sample_interval):
    """Get the training data from the global model

    :param filename: the input data file
    :param warmup_period: warmup period for pipeline data
    :param tpcc_hack: whether to manually fix the tpcc features
    :param ee_sample_interval: sampling interval for the EE OUs
    :param txn_sample_interval: sampling interval for the transaction OUs
    :return: the list of global model data
    """

    if "txn" in filename:
        # Cannot handle the transaction manager data yet
        return []
    if "execution" in filename:
        # Special handle of the execution data
        return _execution_get_grouped_op_unit_data(filename, ee_sample_interval)
    if "pipeline" in filename:
        # Special handle of the pipeline execution data
        return _pipeline_get_grouped_op_unit_data(filename, warmup_period, tpcc_hack, ee_sample_interval)
    if "gc" in filename or "log" in filename:
        # Handle of the gc or log data with interval-based conversion
        return _interval_get_grouped_op_unit_data(filename)

    return _default_get_global_data(filename)


def _execution_get_grouped_op_unit_data(filename, ee_sample_interval):
    # Get the global running data for the execution engine
    data_list = []
    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        for line in tqdm.tqdm(reader):
            # The first element is always the query/pipeline identifier
            identifier = line[0]
            if identifier in query_info.FEATURE_MAP:
                # Need to deep copy since we're going to add the execution mode after it
                opunit_features = copy.deepcopy(query_info.FEATURE_MAP[identifier])

                # get memory scaling factor
                mem_scaling_factor = 1.0
                if line[0] in query_info.MEM_ADJUST_MAP:
                    mem_scaling_factor = query_info.MEM_ADJUST_MAP[line[0]]

                # Execution mode is the second element for now...
                mode = int(line[1])
                for opunit_feature in opunit_features:
                    if opunit_feature[0] in data_info.MEM_ADJUST_OPUNITS:
                        opunit_feature[1].append(mem_scaling_factor)
                    else:
                        opunit_feature[1].append(1.0)
                    opunit_feature[1].append(mode)

                line_data = list(map(int, line[2:]))
                data_list.append(GroupedOpUnitData(line[0], opunit_features, np.array(line_data), ee_sample_interval))

    return data_list


def _pipeline_get_grouped_op_unit_data(filename, warmup_period, tpcc_hack, ee_sample_interval):
    # Get the global running data for the execution engine
    execution_mode_index = data_info.RAW_EXECUTION_MODE_INDEX
    features_vector_index = data_info.RAW_FEATURES_VECTOR_INDEX
    start_time = None

    data_list = []
    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter=",", skipinitialspace=True)
        next(reader)
        for line in reader:
            # extract the time
            cpu_time = line[data_info.RAW_CPU_TIME_INDEX]
            if start_time is None:
                start_time = cpu_time

            if int(cpu_time) - int(start_time) < warmup_period * 1000000:
                continue

            # drop query_id, pipeline_id, num_features, features_vector
            record = [d for i,d in enumerate(line) if i > features_vector_index]
            record.insert(data_info.EXECUTION_MODE_INDEX, line[execution_mode_index])
            data = list(map(data_util.convert_string_to_numeric, record))
            x_multiple = data[:data_info.RECORD_FEATURES_END]
            metrics = np.array(data[-data_info.METRICS_OUTPUT_NUM:])

            # Get the opunits located within
            opunits = []
            features = line[features_vector_index].split(';')
            for idx, feature in enumerate(features):
                if feature == 'LIMIT':
                    continue
                opunit = OpUnit[feature]
                x_loc = [v[idx] if type(v) == list else v for v in x_multiple]

                q_id = int(line[0])
                p_id = int(line[1])
                if tpcc_hack:
                    x_loc = tpcc_fixer.transform_feature(feature, q_id, p_id, x_loc)

                if x_loc[data_info.TUPLE_NUM_INDEX] == 0:
                    logging.info("Skipping {} OU with 0 tuple num".format(opunit.name))
                    continue

                opunits.append((opunit, x_loc))

            if len(opunits) == 0:
                continue
            data_list.append(GroupedOpUnitData("q{} p{}".format(line[0], line[1]), opunits, np.array(metrics),
                                               ee_sample_interval))

    return data_list


def _interval_get_grouped_op_unit_data(filename):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    df = pd.read_csv(filename)
    file_name = os.path.splitext(os.path.basename(filename))[0]

    x = df.iloc[:, :-data_info.METRICS_OUTPUT_NUM].values
    y = df.iloc[:, -data_info.MINI_MODEL_TARGET_NUM:].values
    start_times = df.iloc[:, data_info.TARGET_CSV_INDEX[data_info.Target.START_TIME]].values
    cpu_ids = df.iloc[:, data_info.TARGET_CSV_INDEX[data_info.Target.CPU_ID]].values

    interval = data_info.PERIODIC_OPUNIT_INTERVAL

    # Map from interval start time to the data in this interval
    interval_x_map = {}
    interval_y_map = {}
    interval_cpu_map = {}
    n = x.shape[0]
    for i in tqdm.tqdm(list(range(n)), desc="Group data by interval"):
        rounded_time = data_util.round_to_interval(start_times[i], interval)
        if rounded_time not in interval_x_map:
            interval_x_map[rounded_time] = []
            interval_y_map[rounded_time] = []
        interval_x_map[rounded_time].append(x[i])
        interval_y_map[rounded_time].append(y[i])
        interval_cpu_map[rounded_time] = cpu_ids[i]

    # Construct the new data
    opunit = OpUnit[file_name.upper()]
    data_list = []
    for rounded_time in interval_x_map:
        # Sum the features
        x_new = np.sum(interval_x_map[rounded_time], axis=0)
        # Keep the interval parameter the same
        # TODO: currently the interval parameter is always the last. Change the hard-coding later
        x_new[-1] /= len(interval_x_map[rounded_time])
        # Change all the opunits in the group for this interval to be the new feature
        opunits = [(opunit, x_new)]
        # The prediction is the average behavior
        y_new = np.average(interval_y_map[rounded_time], axis=0)
        n = len(interval_x_map[rounded_time])
        for i in range(n):
            metrics = np.concatenate(([rounded_time + i * interval // n], [interval_cpu_map[rounded_time]], y_new))
            data_list.append(GroupedOpUnitData("{}".format(file_name), opunits, metrics))

    return data_list


def _default_get_global_data(filename):
    # In the default case, the data does not need any pre-processing and the file name indicates the opunit
    return []


class GroupedOpUnitData:
    """
    The class that stores the information about a group of operating units measured together
    """
    def __init__(self, name, opunit_features, metrics, sample_interval=0):
        """
        :param name: The name of the data point (e.g., could be the pipeline identifier)
        :param opunit_features: The list of opunits and their inputs for this event
        :param metrics: The runtime metrics
        :param sample_interval: The sampling interval for this OU group
        """
        self.name = name
        self.opunit_features = opunit_features
        self.y = metrics[-data_info.MINI_MODEL_TARGET_NUM:]
        self.y_pred = None
        index_map = data_info.TARGET_CSV_INDEX
        self.start_time = metrics[index_map[Target.START_TIME]]
        self.end_time = self.start_time + self.y[index_map[Target.ELAPSED_US]] - 1
        self.cpu_id = int(metrics[index_map[Target.CPU_ID]])
        self.sample_interval = sample_interval

    def get_start_time(self, concurrent_counting_mode):
        """Get the start time for this group for counting the concurrent operations

        :param concurrent_counting_mode: ConcurrentCountingMode type
        :return: the start time
        """
        start_time = None
        if concurrent_counting_mode is ConcurrentCountingMode.EXACT:
            start_time = self.start_time
        if concurrent_counting_mode is ConcurrentCountingMode.ESTIMATED:
            start_time = self.start_time
        if concurrent_counting_mode is ConcurrentCountingMode.INTERVAL:
            start_time = self.start_time + global_model_config.INTERVAL_START
        return start_time

    def get_end_time(self, concurrent_counting_mode):
        """Get the end time for this group for counting the concurrent operations

        :param concurrent_counting_mode: ConcurrentCountingMode type
        :return: the end time
        """
        end_time = None
        if concurrent_counting_mode is ConcurrentCountingMode.EXACT:
            end_time = self.end_time
        if concurrent_counting_mode is ConcurrentCountingMode.ESTIMATED:
            end_time = self.start_time + self.y_pred[data_info.TARGET_CSV_INDEX[Target.ELAPSED_US]] - 1
        if concurrent_counting_mode is ConcurrentCountingMode.INTERVAL:
            end_time = self.start_time + global_model_config.INTERVAL_START + global_model_config.INTERVAL_SIZE
        return end_time
