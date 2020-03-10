#!/usr/bin/env python3

import numpy as np
import argparse
import pickle
import logging

import data_info
import io_util
import logging_util
import global_model_util
import training_util
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
        data_list = global_model_util.get_grouped_opunit_data_with_prediction(self.input_path, self.mini_model_map,
                                                                              self.model_results_path)
        logging.info("Finish data loading")
        resource_data_list, impact_data_list = global_model_util.construct_interval_based_global_model_data(
            data_list, self.model_results_path)
        logging.info("Finish constructing the global data")

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

        # Result files
        metrics_path = "{}/global_resource_model_metrics.csv".format(self.model_results_path)
        prediction_path = "{}/global_resource_model_prediction.csv".format(self.model_results_path)
        training_util.create_metrics_and_prediction_files(metrics_path, prediction_path)

        # Predict
        y_pred = self.global_resource_model.predict(x)
        ratio_error = np.average(np.abs(y - y_pred) / (y + 1e-6), axis=0)
        io_util.write_csv_result(metrics_path, "Ratio Error", ratio_error)
        training_util.record_predictions((x, y, y_pred), prediction_path)

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
        for d in impact_data_list:
            mini_model_y_pred = d.target_grouped_op_unit_data.y_pred
            predicted_elapsed_us = mini_model_y_pred[data_info.target_csv_index[Target.ELAPSED_US]]
            x.append(np.concatenate((mini_model_y_pred / predicted_elapsed_us, d.resource_data.y_pred,
                                     d.resource_util_same_core_x)))
            # x.append(np.concatenate((mini_model_y_pred / predicted_elapsed_us, d.global_resource_util_y_pred)))
            y.append(d.target_grouped_op_unit_data.y / (d.target_grouped_op_unit_data.y_pred + 1e-6))

        # Predict
        x = np.array(x)
        y = np.array(y)
        y_pred = self.global_impact_model.predict(x)

        # Record results
        metrics_path = "{}/global_impact_model_metrics.csv".format(self.model_results_path)
        prediction_path = "{}/global_impact_model_prediction.csv".format(self.model_results_path)
        training_util.create_metrics_and_prediction_files(metrics_path, prediction_path)
        ratio_error = np.average(np.abs(y - y_pred) / (y + 1e-6), axis=0)
        io_util.write_csv_result(metrics_path, "Ratio Error", ratio_error)
        training_util.record_predictions((x, y, y_pred), prediction_path)


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
