#!/usr/bin/env python3

import numpy as np
import argparse
import pickle
import logging
import tqdm

import global_model_config
from util import io_util, logging_util
from training_util import global_data_constructing_util, result_writing_util
from info import data_info
from type import Target

np.set_printoptions(precision=4)
np.set_printoptions(edgeitems=10)
np.set_printoptions(suppress=True)


class EndtoendEstimator:
    """
    Trainer for the mini models
    """

    def __init__(self, input_path, model_results_path, mini_model_map, global_resource_model,
                 global_impact_model, global_direct_model, ee_sample_interval, txn_sample_interval,
                 network_sample_interval):
        self.input_path = input_path
        self.model_results_path = model_results_path
        self.mini_model_map = mini_model_map
        self.global_resource_model = global_resource_model
        self.global_impact_model = global_impact_model
        self.global_direct_model = global_direct_model
        self.ee_sample_interval = ee_sample_interval
        self.txn_sample_interval = txn_sample_interval
        self.network_sample_interval = network_sample_interval

    def estimate(self):
        """Train the mini-models

        :return: the map of the trained models
        """
        resource_data_list, impact_data_list = global_data_constructing_util.get_data(self.input_path,
                                                                                      self.mini_model_map,
                                                                                      self.model_results_path,
                                                                                      0,
                                                                                      False,
                                                                                      False,
                                                                                      False,
                                                                                      self.ee_sample_interval,
                                                                                      self.txn_sample_interval,
                                                                                      self.network_sample_interval)
        return self._global_model_prediction(resource_data_list, impact_data_list)

    def _global_model_prediction(self, resource_data_list, impact_data_list):
        """Use the global models to predict

        :param resource_data_list: list of GlobalResourceData
        :param impact_data_list: list of GlobalImpactData
        """
        # First apply the global resource prediction model
        # Get the features and labels
        x = np.array([d.x for d in resource_data_list])
        y = np.array([d.y for d in resource_data_list])
        # Predict
        y_pred = self.global_resource_model.predict(x)

        self._record_results(x, y, y_pred, None, None, "resource", None)

        # Put the prediction global resource util back to the GlobalImpactData
        for i, data in enumerate(resource_data_list):
            data.y_pred = y_pred[i]

        self._model_prediction_with_derived_data(impact_data_list, "impact", self.global_impact_model)

        self._model_prediction_with_derived_data(impact_data_list, "direct", self.global_direct_model)

    def _model_prediction_with_derived_data(self, impact_data_list, model_name, model):
        # Then apply the global impact model
        x = []
        y = []
        mini_model_y_pred = []  # The labels directly predicted from the mini models
        raw_y = []  # The actual labels
        data_list = []
        # The input feature is (normalized mini model prediction, predicted global resource util, the predicted
        # resource util on the same core that the opunit group runs)
        # The output target is the ratio between the actual resource util (including the elapsed time) and the
        # normalized mini model prediction
        for d in tqdm.tqdm(impact_data_list, desc="Construct data for the {} model".format(model_name)):
            data_list.append(d.target_grouped_op_unit_data)
            mini_model_y_pred.append(d.target_grouped_op_unit_data.y_pred)
            raw_y.append(d.target_grouped_op_unit_data.y)
            predicted_elapsed_us = mini_model_y_pred[-1][data_info.instance.target_csv_index[Target.ELAPSED_US]]
            predicted_resource_util = None
            if model_name == "impact":
                predicted_resource_util = d.get_y_pred()
            if model_name == "direct":
                predicted_resource_util = d.x
            # Remove the OU group itself from the total resource data
            self_resource = (mini_model_y_pred[-1] * max(1, d.target_grouped_op_unit_data.concurrency) /
                             len(d.resource_data_list) / global_model_config.INTERVAL_SIZE)
            predicted_resource_util[:mini_model_y_pred[-1].shape[0]] -= self_resource
            predicted_resource_util[predicted_resource_util < 0] = 0
            x.append(np.concatenate((mini_model_y_pred[-1] / predicted_elapsed_us,
                                     predicted_resource_util,
                                     d.resource_util_same_core_x)))
            # x.append(np.concatenate((mini_model_y_pred[-1] / predicted_elapsed_us, predicted_resource_util)))
            y.append(d.target_grouped_op_unit_data.y / (d.target_grouped_op_unit_data.y_pred +
                                                        global_model_config.RATIO_DIVISION_EPSILON))

        # Predict
        x = np.array(x)
        y = np.array(y)
        y_pred = model.predict(x)

        # Record results
        self._record_results(x, y, y_pred, raw_y, mini_model_y_pred, model_name, data_list)

    def _record_results(self, x, y, y_pred, raw_y, mini_model_y_pred, label, data_list):
        """Record the prediction results

        :param x: the input data
        :param y: the actual output
        :param y_pred: the predicted output
        :param label: the result label ("resource" or "impact")
        """
        # Result files
        metrics_path = "{}/global_{}_model_metrics.csv".format(self.model_results_path, label)
        prediction_path = "{}/global_{}_model_prediction.csv".format(self.model_results_path, label)
        result_writing_util.create_metrics_and_prediction_files(metrics_path, prediction_path, True)

        # Log the prediction results
        ratio_error = np.average(np.abs(y - y_pred) / (y + 1e-6), axis=0)
        io_util.write_csv_result(metrics_path, "Model Ratio Error", ratio_error)
        result_writing_util.record_predictions((x, y_pred, y), prediction_path)

        # Print Error summary to command line
        if label == "resource":
            avg_original_ratio_error = np.average(np.abs(y - x[:, :y.shape[1]]) / (y + 1e-6), axis=0)
        else:
            avg_original_ratio_error = np.average(np.abs(1 / (y + 1e-6) - 1), axis=0)
        logging.info('Model Original Ratio Error ({}): {}'.format(label, avg_original_ratio_error))
        logging.info('Model Ratio Error ({}): {}'.format(label, ratio_error))
        logging.info('')

        if label != "resource":
            # Calculate the accumulated ratio error
            epsilon = global_model_config.RATIO_DIVISION_EPSILON
            mini_model_y_pred = np.array(mini_model_y_pred)
            raw_y = np.array(raw_y)
            raw_y_pred = (mini_model_y_pred + epsilon) * y_pred
            accumulated_raw_y = np.sum(raw_y, axis=0)
            accumulated_raw_y_pred = np.sum(raw_y_pred, axis=0)
            original_ratio_error = np.abs(raw_y - mini_model_y_pred) / (raw_y + epsilon)
            avg_original_ratio_error = np.average(original_ratio_error, axis=0)
            ratio_error = np.abs(raw_y - raw_y_pred) / (raw_y + epsilon)
            avg_ratio_error = np.average(ratio_error, axis=0)
            accumulated_percentage_error = np.abs(accumulated_raw_y - accumulated_raw_y_pred) / (
                    accumulated_raw_y + epsilon)
            original_accumulated_percentage_error = np.abs(accumulated_raw_y - np.sum(mini_model_y_pred, axis=0)) / (
                    accumulated_raw_y + epsilon)

            logging.info('Original Ratio Error: {}'.format(avg_original_ratio_error))
            io_util.write_csv_result(metrics_path, "Original Ratio Error", avg_original_ratio_error)
            logging.info('Ratio Error: {}'.format(avg_ratio_error))
            io_util.write_csv_result(metrics_path, "Ratio Error", avg_ratio_error)
            logging.info('Original Accumulated Ratio Error: {}'.format(original_accumulated_percentage_error))
            io_util.write_csv_result(metrics_path, "Original Accumulated Ratio Error",
                                     original_accumulated_percentage_error)
            logging.info('Accumulated Ratio Error: {}'.format(accumulated_percentage_error))
            io_util.write_csv_result(metrics_path, "Accumulated Ratio Error", accumulated_percentage_error)
            logging.info('Accumulated Actual: {}'.format(accumulated_raw_y))
            logging.info('Original Accumulated Predict: {}'.format(np.sum(mini_model_y_pred, axis=0)))
            logging.info('Accumulated Predict: {}'.format(accumulated_raw_y_pred))

            if label == 'direct':
                prediction_path = "{}/grouped_opunit_prediction.csv".format(self.model_results_path)
                io_util.create_csv_file(prediction_path, ["Pipeline", "", "Actual", "", "Predicted", "", "Ratio Error"])
                for i, data in enumerate(data_list):
                    io_util.write_csv_result(prediction_path, data.name, [""] + list(raw_y[i]) + [""] +
                                             list(raw_y_pred[i]) + [""] + list(ratio_error[i]))

                average_result_path = "{}/interval_average_prediction.csv".format(self.model_results_path)
                io_util.create_csv_file(average_result_path,
                                        ["Timestamp", "Actual Average", "Predicted Average"])

                interval_y_map = {}
                interval_y_pred_map = {}
                mark_list = None
                # mark_list = _generate_mark_list(data_list)
                for i, data in enumerate(data_list):
                    # Don't count the create index OU
                    # TODO(lin): needs better way to evaluate... maybe add a id_query field to GroupedOpunitData
                    if data.concurrency > 0:
                        continue
                    if mark_list is not None and not mark_list[i]:
                        continue
                    interval_time = _round_to_interval(data.start_time, global_model_config.AVERAGING_INTERVAL)
                    if interval_time not in interval_y_map:
                        interval_y_map[interval_time] = []
                        interval_y_pred_map[interval_time] = []
                    interval_y_map[interval_time].append(raw_y[i][-5])
                    interval_y_pred_map[interval_time].append(raw_y_pred[i][-5])

                for time in sorted(interval_y_map.keys()):
                    if mark_list is None:
                        io_util.write_csv_result(average_result_path, time,
                                                 [np.average(interval_y_map[time]),
                                                  np.average(interval_y_pred_map[time])])
                    else:
                        io_util.write_csv_result(average_result_path, time,
                                                 [np.sum(interval_y_map[time]),
                                                  np.sum(interval_y_pred_map[time])])


def _round_to_interval(time, interval):
    """
    :param time: in us
    :return: time in us rounded to the earliest interval
    """
    return time - time % interval


def _generate_mark_list(data_list):
    """Generate a mark list to filter a specific operation

    :param data_list:
    :return:
    """
    mark_list = []
    name_set = {'q3007', 'q2994', 'q3006', 'q2972', 'q3003', 'q2963', 'q2989', 'q2901', 'q2994', 'q3001'}
    for d in data_list:
        if d.name[:5] in name_set:
            mark_list.append(True)
        else:
            mark_list.append(False)

    return mark_list


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='End-to-end Estimator')
    aparser.add_argument('--input_path', default='endtoend_input',
                         help='Input file path for the endtoend estimator')
    aparser.add_argument('--model_results_path', default='endtoend_estimation_results',
                         help='Prediction results of the mini models')
    aparser.add_argument('--mini_model_file', default='trained_model/mini_model_map.pickle',
                         help='File of the saved mini models')
    aparser.add_argument('--global_resource_model_file',
                         default='trained_model/global_resource_model.pickle',
                         help='File of the saved global resource model')
    aparser.add_argument('--global_impact_model_file',
                         default='trained_model/global_impact_model.pickle',
                         help='File of the saved global impact model')
    aparser.add_argument('--global_direct_model_file',
                         default='trained_model/global_direct_model.pickle',
                         help='File of the saved global impact model')
    aparser.add_argument('--ee_sample_interval', type=int, default=49,
                         help='Sampling interval for the execution engine OUs')
    aparser.add_argument('--txn_sample_interval', type=int, default=49,
                         help='Sampling interval for the transaction OUs')
    aparser.add_argument('--network_sample_interval', type=int, default=49,
                         help='Sampling interval for the network OUs')
    aparser.add_argument('--log', default='info', help='The logging level')
    args = aparser.parse_args()

    logging_util.init_logging(args.log)

    with open(args.mini_model_file, 'rb') as pickle_file:
        model_map, data_info.instance = pickle.load(pickle_file)
    with open(args.global_resource_model_file, 'rb') as pickle_file:
        resource_model = pickle.load(pickle_file)
    with open(args.global_impact_model_file, 'rb') as pickle_file:
        impact_model = pickle.load(pickle_file)
    with open(args.global_direct_model_file, 'rb') as pickle_file:
        direct_model = pickle.load(pickle_file)
    estimator = EndtoendEstimator(args.input_path, args.model_results_path, model_map, resource_model, impact_model,
                                  direct_model, args.ee_sample_interval, args.txn_sample_interval,
                                  args.network_sample_interval)
    estimator.estimate()
