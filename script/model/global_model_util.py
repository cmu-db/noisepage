import logging
import glob
import os
import numpy as np

import hardware_info
import io_util
import data_info
import grouped_op_unit_data
import global_model_data
from type import Target, OpUnit, ConcurrentCountingMode


def get_grouped_opunit_data_with_prediction(input_path, mini_model_map, model_results_path):
    """Get the grouped opunit data with the predicted metrics and elapsed time

    :param input_path: input data file path
    :param mini_model_map: mini models used for prediction
    :param model_results_path:
    :return: The list of the GroupedOpUnitData objects
    """
    data_list = _get_data_list(input_path)
    _predict_grouped_opunit_data(data_list, mini_model_map, model_results_path)
    return data_list


def construct_global_model_data(data_list, model_results_path, concurrent_counting_mode):
    """Construct the GlobalModelData used for the global model training

    :param data_list: The list of GroupedOpUnitData objects
    :param model_results_path: directory path to log the result information
    :param concurrent_counting_mode: how to count the concurrent running operations
    :return: The list of the GlobalModelData objects
    """
    prediction_path = "{}/global_resource_data.csv".format(model_results_path)
    io_util.create_csv_file(prediction_path, ["Elapsed us", "# Concurrent OpUnit Groups"])

    global_model_data_list = []

    # Define a secondary_counting_mode corresponding to the concurrent_counting_mode to derive the concurrent operations
    # in different scenarios
    secondary_counting_mode = concurrent_counting_mode
    if concurrent_counting_mode is ConcurrentCountingMode.INTERVAL:
        secondary_counting_mode = ConcurrentCountingMode.ESTIMATED

    start_time_idx = [i[0] for i in sorted(enumerate(data_list), key=lambda a: a[1].get_start_time(
        secondary_counting_mode))]
    # The concurrent running opunit groups that that has already started
    started_data_list = []
    for i, idx in enumerate(start_time_idx):
        data = data_list[idx]

        # First find the overlap between the current opunit group and the previously started groups
        started_data_list.append(data)
        concurrent_data_list = []
        for started_data in started_data_list:
            if started_data.get_end_time(secondary_counting_mode) >= data.get_start_time(concurrent_counting_mode):
                # The entered_data overlaps with the current opunit group
                concurrent_data_list.append(started_data)
        started_data_list = concurrent_data_list.copy()

        # Then find the overlap with the opunit groups started later
        j = i + 1
        while j < len(start_time_idx) and data_list[start_time_idx[j]].get_start_time(secondary_counting_mode) <= \
                data.get_end_time(concurrent_counting_mode):
            concurrent_data_list.append(data_list[start_time_idx[j]])
            j += 1

        x, same_core_x, y = _get_global_resource_util(data, concurrent_data_list, prediction_path,
                                                      concurrent_counting_mode)
        global_model_data_list.append(global_model_data.GlobalModelData(data, concurrent_data_list, x,
                                                                        same_core_x, y))

    return global_model_data_list


def _get_global_resource_util(grouped_opunit, concurrent_data_list, prediction_path, concurrent_counting_mode):
    """Get the input feature and the target output for the global resource utilization metrics during the time range
    of a GroupedOpUnitData

    The calculation is adjusted by the overlapping ratio between the opunit groups and the time range.

    :param grouped_opunit: the data to calculate the global resource utilization for
    :param concurrent_data_list: the concurrent running opunit groups
    :param prediction_path: the file path to log the prediction results
    :param concurrent_counting_mode: how to count the concurrent running operations
    :return: (the input feature, the resource utilization on the other logical core of the same physical core,
    the output resource targets)
    """
    # Define a secondary_counting_mode corresponding to the concurrent_counting_mode to derive the concurrent operations
    # in different scenarios
    secondary_counting_mode = concurrent_counting_mode
    if concurrent_counting_mode is ConcurrentCountingMode.INTERVAL:
        secondary_counting_mode = ConcurrentCountingMode.ESTIMATED

    start_time = grouped_opunit.get_start_time(concurrent_counting_mode)
    end_time = grouped_opunit.get_end_time(concurrent_counting_mode)
    elapsed_us = end_time - start_time + 1

    # The adjusted resource metrics per logical core.
    # TODO: Assuming each physical core has two logical cores via hyper threading for now. Can extend to other scenarios
    physical_core_num = hardware_info.physical_core_num
    adjusted_x_list = [0] * 2 * physical_core_num
    adjusted_y = 0
    logging.debug(concurrent_data_list)
    logging.debug("{} {}".format(start_time, end_time))

    for data in concurrent_data_list:
        data_start_time = data.get_start_time(secondary_counting_mode)
        data_end_time = data.get_end_time(secondary_counting_mode)
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
    cpu_id = grouped_opunit.cpu_id
    same_core_adjusted_x = adjusted_x_list[cpu_id - physical_core_num if cpu_id > physical_core_num else cpu_id]

    memory_idx = data_info.target_csv_index[Target.MEMORY_B]
    # FIXME: Using dummy memory value for now. Eventually we need to transfer the memory estimation between pipelines
    adjusted_y[memory_idx] = 1
    sum_adjusted_x[memory_idx] = 1
    std_adjusted_x[memory_idx] = 1
    same_core_adjusted_x[memory_idx] = 1

    ratio_error = abs(adjusted_y - sum_adjusted_x) / (adjusted_y + 1e-6)

    logging.debug(sum_adjusted_x)
    logging.debug(adjusted_y)
    logging.debug("")

    io_util.write_csv_result(prediction_path, elapsed_us, [len(concurrent_data_list)] + list(sum_adjusted_x) + [""] +
                             list(adjusted_y) + [""] + list(ratio_error))

    adjusted_x = np.concatenate((sum_adjusted_x, std_adjusted_x))

    return adjusted_x, same_core_adjusted_x, adjusted_y


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
        print(filename)
        data_list += grouped_op_unit_data.get_grouped_op_unit_data(filename)
        # break

    return data_list


def _predict_grouped_opunit_data(data_list, mini_model_map, model_results_path):
    """Use the mini-runner to predict the resource consumptions for all the GlobalData, and record the prediction
    result in place

    :param data_list: The list of the GroupedOpUnitData objects
    :param mini_model_map: The trained mini models
    :param model_results_path: file path to log the prediction results
    """
    prediction_path = "{}/grouped_opunit_prediction.csv".format(model_results_path)
    io_util.create_csv_file(prediction_path, ["Pipeline", "Actual", "Predicted", "Ratio Error"])

    # Have to use a prediction cache when having lots of global data...
    prediction_cache = {}

    # First run a prediction on the global running data with the mini model results
    for i, data in enumerate(data_list):
        # if i == 10:
        #    break
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
                prediction_cache[key] = y_pred
                # subtract scan from certain double-counted opunits
                if opunit in data_info.scan_subtract_opunits:
                    scan_y_pred = mini_model_map[OpUnit.SCAN].predict(x)
                    y_pred -= scan_y_pred
            else:
                y_pred = prediction_cache[key]
            logging.debug("Predicted {} elapsed time with feature {}: {}".format(opunit_feature[0].name,
                                                                                 x[0], y_pred[0, -1]))
            pipeline_y_pred += y_pred[0]

        # Record the predicted
        data.y_pred = pipeline_y_pred
        logging.debug("{} pipeline predicted time: {}".format(data.name, pipeline_y_pred[-1]))
        ratio_error = abs(y[-1] - pipeline_y_pred[-1]) / y[-1]
        logging.debug("|Actual - Predict| / Actual: {}".format(ratio_error))

        io_util.write_csv_result(prediction_path, data.name + " " + str(x[0][-1]),
                                 [y[-1], pipeline_y_pred[-1], ratio_error])

        logging.debug("")
