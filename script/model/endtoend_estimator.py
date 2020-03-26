#!/usr/bin/env python3

import numpy as np
import argparse
import pickle
import logging
import tqdm

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
                 global_impact_model):
        self.input_path = input_path
        self.model_results_path = model_results_path
        self.mini_model_map = mini_model_map
        self.global_resource_model = global_resource_model
        self.global_impact_model = global_impact_model

    def estimate(self):
        """Train the mini-models

        :return: the map of the trained models
        """
        resource_data_list, impact_data_list = global_data_constructing_util.get_data(self.input_path, self.mini_model_map,
                                                                                      self.model_results_path)
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

        self._record_results(x, y, y_pred, "resource")

        # Put the prediction global resource util back to the GlobalImpactData
        for i, data in enumerate(resource_data_list):
            data.y_pred = y_pred[i]

        # Then apply the global impact model
        x = []
        y = []
        # The input feature is (normalized mini model prediction, predicted global resource util, the predicted
        # resource util on the same core that the opunit group runs)
        # The output target is the ratio between the actual resource util (including the elapsed time) and the
        # normalized mini model prediction
        for d in tqdm.tqdm(impact_data_list, desc="Construct data for the impact model"):
            mini_model_y_pred = d.target_grouped_op_unit_data.y_pred
            predicted_elapsed_us = mini_model_y_pred[data_info.TARGET_CSV_INDEX[Target.ELAPSED_US]]
            x.append(np.concatenate((mini_model_y_pred / predicted_elapsed_us, d.resource_data.y_pred,
                                     d.resource_util_same_core_x)))
            # x.append(np.concatenate((mini_model_y_pred / predicted_elapsed_us, d.global_resource_util_y_pred)))
            y.append(d.target_grouped_op_unit_data.y / (d.target_grouped_op_unit_data.y_pred + 1e-6))

            memory_idx = data_info.TARGET_CSV_INDEX[Target.MEMORY_B]
            # FIXME: fix the dummy memory value later
            x[-1][mini_model_y_pred.shape[0] + memory_idx] = 1
            y[-1][memory_idx] = 1

        # Predict
        x = np.array(x)
        y = np.array(y)
        y_pred = self.global_impact_model.predict(x)

        # Record results
        self._record_results(x, y, y_pred, "impact")

    def _record_results(self, x, y, y_pred, label):
        """Record the prediction results

        :param x: the input data
        :param y: the actual output
        :param y_pred: the predicted output
        :param label: the result label ("resource" or "impact")
        """
        # Result files
        metrics_path = "{}/global_{}_model_metrics.csv".format(self.model_results_path, label)
        prediction_path = "{}/global_{}_model_prediction.csv".format(self.model_results_path, label)
        result_writing_util.create_metrics_and_prediction_files(metrics_path, prediction_path)

        # Log the prediction results
        ratio_error = np.average(np.abs(y - y_pred) / (y + 1e-6), axis=0)
        io_util.write_csv_result(metrics_path, "Ratio Error", ratio_error)
        result_writing_util.record_predictions((x, y_pred, y), prediction_path)

        # Print Error summary to command line
        if label == "resource":
            original_ratio_error = np.average(np.abs(y - x[:, :y.shape[1]]) / (y + 1e-6), axis=0)
        else:
            original_ratio_error = np.average(np.abs(1/(y+1e-6) - 1), axis=0)
        logging.info('Original Ratio Error: {}'.format(original_ratio_error))
        logging.info('Ratio Error: {}'.format(ratio_error))


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='End-to-end Estimator')
    aparser.add_argument('--input_path', default='endtoend_input', help='Input file path for the endtoend estimator')
    aparser.add_argument('--model_results_path', default='endtoend_estimation_results',
                         help='Prediction results of the mini models')
    aparser.add_argument('--mini_model_file', default='trained_model/mini_model_map.pickle',
                         help='File of the saved mini models')
    aparser.add_argument('--global_resource_model_file', default='trained_model/global_resource_model.pickle',
                         help='File of the saved global resource model')
    aparser.add_argument('--global_impact_model_file', default='trained_model/global_impact_model.pickle',
                         help='File of the saved global impact model')
    aparser.add_argument('--log', default='info', help='The logging level')
    args = aparser.parse_args()

    logging_util.init_logging(args.log)

    with open(args.mini_model_file, 'rb') as pickle_file:
        model_map = pickle.load(pickle_file)
    with open(args.global_resource_model_file, 'rb') as pickle_file:
        resource_model = pickle.load(pickle_file)
    with open(args.global_impact_model_file, 'rb') as pickle_file:
        impact_model = pickle.load(pickle_file)
    estimator = EndtoendEstimator(args.input_path, args.model_results_path, model_map, resource_model, impact_model)
    estimator.estimate()
