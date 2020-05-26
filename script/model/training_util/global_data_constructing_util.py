import logging
import copy
import glob
import math
import os
import numpy as np
import tqdm
import pickle

from util import io_util
from info import data_info, hardware_info
from data_class import global_model_data, grouped_op_unit_data
import global_model_config
from type import Target, OpUnit, ConcurrentCountingMode


def get_data(input_path, mini_model_map, model_results_path):
    """Get the data for the global models

    Read from the cache if exists, otherwise save the constructed data to the cache.

    :param input_path: input data file path
    :param mini_model_map: mini models used for prediction
    :param model_results_path: directory path to log the result information
    :return: (GlobalResourceData list, GlobalImpactData list)
    """
    cache_file = input_path + '/global_model_data.pickle'
    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as pickle_file:
            resource_data_list, impact_data_list = pickle.load(pickle_file)
    else:
        data_list = _get_grouped_opunit_data_with_prediction(input_path, mini_model_map, model_results_path)
        resource_data_list, impact_data_list = _construct_interval_based_global_model_data(data_list,
                                                                                           model_results_path)
        with open(cache_file, 'wb') as file:
            pickle.dump((resource_data_list, impact_data_list), file)

    return resource_data_list, impact_data_list


def _get_grouped_opunit_data_with_prediction(input_path, mini_model_map, model_results_path):
    """Get the grouped opunit data with the predicted metrics and elapsed time

    :param input_path: input data file path
    :param mini_model_map: mini models used for prediction
    :param model_results_path: directory path to log the result information
    :return: The list of the GroupedOpUnitData objects
    """
    data_list = _get_data_list(input_path)
    _predict_grouped_opunit_data(data_list, mini_model_map, model_results_path)
    logging.info("Finished GroupedOpUnitData prediction with the mini models")
    return data_list


def _construct_interval_based_global_model_data(data_list, model_results_path):
    """Construct the GlobalImpactData used for the global model training

    :param data_list: The list of GroupedOpUnitData objects
    :param model_results_path: directory path to log the result information
    :return: (GlobalResourceData list, GlobalImpactData list)
    """
    prediction_path = "{}/global_resource_data.csv".format(model_results_path)
    io_util.create_csv_file(prediction_path, ["Elapsed us", "# Concurrent OpUnit Groups"])

    start_time_list = sorted([d.get_start_time(ConcurrentCountingMode.INTERVAL) for d in data_list])
    rounded_start_time_list = [_round_to_second(start_time_list[0])]
    # Map from interval start time to the data in this interval
    interval_data_map = {rounded_start_time_list[0]: []}
    # Get all the interval start times and initialize the map
    for t in start_time_list:
        rounded_time = _round_to_second(t)
        if rounded_time > rounded_start_time_list[-1]:
            rounded_start_time_list.append(rounded_time)
            interval_data_map[rounded_time] = []

    for data in tqdm.tqdm(data_list, desc="Find Interval Data"):
        # For each data, find the intervals that might overlap with it
        interval_start_time = _round_to_second(data.get_start_time(ConcurrentCountingMode.EXACT) -
                                               global_model_config.INTERVAL_SIZE + global_model_config.INTERVAL_SEGMENT)
        while interval_start_time <= data.get_end_time(ConcurrentCountingMode.ESTIMATED):
            if interval_start_time in interval_data_map:
                interval_data_map[interval_start_time].append(data)
            interval_start_time += global_model_config.INTERVAL_SEGMENT

    # Get the global resource data
    resource_data_map = {}
    for start_time in tqdm.tqdm(rounded_start_time_list, desc="Construct GlobalResourceData"):
        resource_data_map[start_time] = _get_global_resource_data(start_time, interval_data_map[start_time],
                                                                  prediction_path)

    # Now construct the global impact data
    impact_data_list = []
    physical_core_num = hardware_info.PHYSICAL_CORE_NUM
    for data in data_list:
        interval_start_time = _round_to_second(data.get_start_time(ConcurrentCountingMode.INTERVAL))
        resource_data = resource_data_map[interval_start_time]
        cpu_id = data.cpu_id
        same_core_x = resource_data.x_list[cpu_id - physical_core_num if cpu_id > physical_core_num else cpu_id]
        memory_idx = data_info.TARGET_CSV_INDEX[Target.MEMORY_B]
        # FIXME: fix the dummy memory value later
        same_core_x[memory_idx] = 1
        impact_data_list.append(global_model_data.GlobalImpactData(data, resource_data, same_core_x))

    return list(resource_data_map.values()), impact_data_list


def _round_to_second(time):
    """
    :param time: in us
    :return: time in us rounded to the earliest second
    """
    return time - time % 1000000


def _get_global_resource_data(start_time, concurrent_data_list, log_path):
    """Get the input feature and the target output for the global resource utilization metrics during an interval

    The calculation is adjusted by the overlapping ratio between the opunit groups and the time range.

    :param start_time: of the interval
    :param concurrent_data_list: the concurrent running opunit groups
    :param log_path: the file path to log the data construction results
    :return: (the input feature, the resource utilization on the other logical core of the same physical core,
    the output resource targets)
    """
    # Define a secondary_counting_mode corresponding to the concurrent_counting_mode to derive the concurrent operations
    # in different scenarios
    end_time = start_time + global_model_config.INTERVAL_SIZE - 1
    elapsed_us = global_model_config.INTERVAL_SIZE

    # The adjusted resource metrics per logical core.
    # TODO: Assuming each physical core has two logical cores via hyper threading for now. Can extend to other scenarios
    physical_core_num = hardware_info.PHYSICAL_CORE_NUM
    adjusted_x_list = [0] * 2 * physical_core_num
    adjusted_y = 0
    logging.debug(concurrent_data_list)
    logging.debug("{} {}".format(start_time, end_time))

    for data in concurrent_data_list:
        data_start_time = data.get_start_time(ConcurrentCountingMode.ESTIMATED)
        data_end_time = data.get_end_time(ConcurrentCountingMode.ESTIMATED)
        ratio = _calculate_range_overlap(start_time, end_time, data_start_time, data_end_time) / (data_end_time -
                                                                                                  data_start_time + 1)
        logging.debug("{} {} {}".format(data_start_time, data_end_time, ratio))
        logging.debug("{} {}".format(data.y, data.y_pred))
        adjusted_y += data.y * ratio
        cpu_id = data.cpu_id
        if cpu_id > physical_core_num:
            cpu_id -= physical_core_num
        adjusted_x_list[cpu_id] += data.y_pred * ratio

    # change the number to per time unit (us) utilization
    for x in adjusted_x_list:
        x /= elapsed_us
    adjusted_y /= elapsed_us

    sum_adjusted_x = np.sum(adjusted_x_list, axis=0)
    std_adjusted_x = np.std(adjusted_x_list, axis=0)

    memory_idx = data_info.TARGET_CSV_INDEX[Target.MEMORY_B]
    # FIXME: Using dummy memory value for now. Eventually we need to transfer the memory estimation between pipelines
    adjusted_y[memory_idx] = 1
    sum_adjusted_x[memory_idx] = 1
    std_adjusted_x[memory_idx] = 1

    ratio_error = abs(adjusted_y - sum_adjusted_x) / (adjusted_y + 1e-6)

    logging.debug(sum_adjusted_x)
    logging.debug(adjusted_y)
    logging.debug("")

    io_util.write_csv_result(log_path, elapsed_us, [len(concurrent_data_list)] + list(sum_adjusted_x) + [""] +
                             list(adjusted_y) + [""] + list(ratio_error))

    adjusted_x = np.concatenate((sum_adjusted_x, std_adjusted_x))

    return global_model_data.GlobalResourceData(start_time, adjusted_x_list, adjusted_x, adjusted_y)


def _calculate_range_overlap(start_timel, end_timel, start_timer, end_timer):
    return min(end_timel, end_timer) - max(start_timel, start_timer) + 1


def _get_data_list(input_path):
    """Get the list of all the operating units (or groups of operating units) stored in GlobalData objects

    :param input_path: input data file path
    :return: the list of all the operating units (or groups of operating units) stored in GlobalData objects
    """
    data_list = []

    # First get the data for all mini runners
    for filename in glob.glob(os.path.join(input_path, '*.csv')):
        data_list += grouped_op_unit_data.get_grouped_op_unit_data(filename)
        logging.info("Loaded file: {}".format(filename))

    return data_list


def _predict_grouped_opunit_data(data_list, mini_model_map, model_results_path):
    """Use the mini-runner to predict the resource consumptions for all the GlobalData, and record the prediction
    result in place

    :param data_list: The list of the GroupedOpUnitData objects
    :param mini_model_map: The trained mini models
    :param model_results_path: file path to log the prediction results
    """
    prediction_path = "{}/grouped_opunit_prediction.csv".format(model_results_path)
    io_util.create_csv_file(prediction_path, ["Pipeline", "Actual Us", "Predicted Us", "Actual Mem", "Predicted Mem", "", "Ratio Error"])

    # Have to use a prediction cache when having lots of global data...
    prediction_cache = {}

    # First run a prediction on the global running data with the mini model results
    for i, data in enumerate(tqdm.tqdm(data_list, desc="Predict GroupedOpUnitData")):
        y = data.y
        logging.debug("{} pipeline elapsed time: {}".format(data.name, y[-1]))
        pipeline_y_pred = 0
        x = None
        for opunit_feature in data.opunit_features:
            opunit = opunit_feature[0]
            opunit_model = mini_model_map[opunit]
            x = np.array(opunit_feature[1]).reshape(1, -1)
            key = (opunit, x.tobytes())
            if key not in prediction_cache:
                y_pred = opunit_model.predict(x)
                y_pred = np.clip(y_pred, 0, None)
                prediction_cache[key] = y_pred
            else:
                y_pred = prediction_cache[key]
            logging.debug("Predicted {} elapsed time with feature {}: {}".format(opunit_feature[0].name,
                                                                                 x[0], y_pred[0, -1]))

            if opunit in data_info.MEM_ADJUST_OPUNITS:
                # Compute the number of "slots" (based on row feature or cardinality feature
                num_tuple = opunit_feature[1][data_info.TUPLE_NUM_INDEX]
                if opunit == OpUnit.AGG_BUILD:
                    num_tuple = opunit_feature[1][data_info.CARDINALITY_INDEX]

                # SORT/AGG/HASHJOIN_BUILD all allocate a "pointer" buffer
                # that contains the first pow2 larger than num_tuple entries
                pow_high = 2 ** math.ceil(math.log(num_tuple, 2))
                buffer_size = pow_high * data_info.POINTER_SIZE
                if opunit == OpUnit.AGG_BUILD and num_tuple <= 256:
                    # For AGG_BUILD, if slots <= AggregationHashTable::K_DEFAULT_INITIAL_TABLE_SIZE
                    # the buffer is not recorded as part of the pipeline
                    buffer_size = 0

                pred_mem = y_pred[0][data_info.TARGET_CSV_INDEX[Target.MEMORY_B]]
                if pred_mem <= buffer_size:
                    logging.warning("{} feature {} {} with prediction {} exceeds buffer {}"
                            .format(data.name, opunit_feature, opunit_feature[1], y_pred[0], buffer_size))

                # Poorly encapsulated, but memory scaling factor is located as the 2nd last of feature
                # slightly inaccurate since ignores load factors for hash tables
                adj_mem = (pred_mem - buffer_size) * opunit_feature[1][-2] + buffer_size

                # Don't modify prediction cache
                y_pred = copy.deepcopy(y_pred)
                y_pred[0][data_info.TARGET_CSV_INDEX[Target.MEMORY_B]] = adj_mem

            pipeline_y_pred += y_pred[0]

        # Record the predicted
        data.y_pred = pipeline_y_pred
        logging.debug("{} pipeline predicted time: {}".format(data.name, pipeline_y_pred[-1]))
        ratio_error = abs(y - pipeline_y_pred) / (y + 1e-6)
        logging.debug("|Actual - Predict| / Actual: {}".format(ratio_error[-1]))

        io_util.write_csv_result(prediction_path, data.name + " " + str(x[0][-1]),
                                 [y[-1], pipeline_y_pred[-1], y[-2], pipeline_y_pred[-2], ""] + list(ratio_error))

        logging.debug("")
