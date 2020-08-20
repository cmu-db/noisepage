#!/usr/bin/env python3

import numpy as np
import argparse
import pickle
import logging
import tqdm
import random
from sklearn import model_selection

import model
from info import data_info
from util import io_util, logging_util
from training_util import global_data_constructing_util, result_writing_util
from type import Target

np.set_printoptions(precision=4)
np.set_printoptions(edgeitems=10)
np.set_printoptions(suppress=True)


def _global_model_training_process(x, y, methods, test_ratio, metrics_path, prediction_path):
    """Training process for the global models

    :param x: input feature
    :param y: labels
    :param methods: ML models to enumerate
    :param test_ratio: train-test split ratio
    :param metrics_path: to store the prediction metrics
    :param prediction_path: to store the raw prediction results
    :return: (the best model, the indices for the test data for additional metric calculation)
    """
    global_model = None
    result_writing_util.create_metrics_and_prediction_files(metrics_path, prediction_path)
    n_samples = x.shape[0]
    indices = np.arange(n_samples)

    x_train, x_test, y_train, y_test, indices_train, indices_test = model_selection.train_test_split(
        x, y, indices, test_size=test_ratio, random_state=0)

    min_percentage_error = 1
    pred_results = None
    elapsed_us_index = data_info.TARGET_CSV_INDEX[Target.ELAPSED_US]

    for method in methods:
        # Train the model
        logging.info("Training the global model with {}".format(method))
        regressor = model.Model(method)
        regressor.train(x_train, y_train)

        # Evaluate on both the training and test set
        results = []
        evaluate_data = [(x_train, y_train), (x_test, y_test)]
        train_test_label = ["Train", "Test"]
        for i, d in enumerate(evaluate_data):
            evaluate_x = d[0]
            evaluate_y = d[1]

            y_pred = regressor.predict(evaluate_x)
            logging.debug("x shape: {}".format(evaluate_x.shape))
            logging.debug("y shape: {}".format(y_pred.shape))
            percentage_error = np.average(np.abs(evaluate_y - y_pred) / (evaluate_y + 1), axis=0)
            results += list(percentage_error) + [""]

            logging.info('{} Ratio Error: {}'.format(train_test_label[i], percentage_error))

            # Record the model with the lowest elapsed time prediction (since that might be the most
            # important prediction)
            if i == 1 and percentage_error[elapsed_us_index] < min_percentage_error:
                min_percentage_error = percentage_error[elapsed_us_index]
                global_model = regressor
                pred_results = (evaluate_x, y_pred, evaluate_y)

        io_util.write_csv_result(metrics_path, method, results)

        logging.info("")

    # Record the best prediction results on the test data
    result_writing_util.record_predictions(pred_results, prediction_path)

    return global_model, indices_test


class GlobalTrainer:
    """
    Trainer for the mini models
    """

    def __init__(self, input_path, model_results_path, ml_models, test_ratio, impact_model_ratio, mini_model_map, warmup_period, simulate_cache, tpcc_hack):
        self.input_path = input_path
        self.model_results_path = model_results_path
        self.ml_models = ml_models
        self.test_ratio = test_ratio
        self.impact_model_ratio = impact_model_ratio
        self.mini_model_map = mini_model_map
        self.warmup_period = warmup_period
        self.simulate_cache = simulate_cache
        self.tpcc_hack = tpcc_hack

    def train(self):
        """Train the mini-models

        :return: the map of the trained models
        """
        resource_data_list, impact_data_list = global_data_constructing_util.get_data(self.input_path,
                                                                                      self.mini_model_map,
                                                                                      self.model_results_path,
                                                                                      self.warmup_period,
                                                                                      self.simulate_cache,
                                                                                      self.tpcc_hack)

        return self._train_global_models(resource_data_list, impact_data_list)

    def _train_global_models(self, resource_data_list, impact_data_list):
        """Train the global models

        :param resource_data_list: list of GlobalResourceData
        :param impact_data_list: list of GlobalImpactData
        :return: (global resource model, global impact model)
        """
        # First train the resource prediction model
        # Get the features and labels
        x = np.array([d.x for d in resource_data_list])
        y = np.array([d.y for d in resource_data_list])

        # Training
        metrics_path = "{}/global_resource_model_metrics.csv".format(self.model_results_path)
        prediction_path = "{}/global_resource_model_prediction.csv".format(self.model_results_path)
        global_resource_model, _ = _global_model_training_process(x, y, self.ml_models, self.test_ratio, metrics_path,
                                                                  prediction_path)

        # Put the prediction global resource util back to the GlobalImpactData
        y_pred = global_resource_model.predict(x)
        for i, data in enumerate(resource_data_list):
            data.y_pred = y_pred[i]

        global_impact_model = self._train_model_with_derived_data(impact_data_list, "impact")

        global_direct_model = self._train_model_with_derived_data(impact_data_list, "direct")

        return global_resource_model, global_impact_model, global_direct_model

    def _train_model_with_derived_data(self, impact_data_list, model_name):
        # Then train the global impact model
        x = []
        y = []
        mini_model_y_pred = []  # The labels directly predicted from the mini models
        raw_y = []  # The actual labels
        data_len = len(impact_data_list)
        sample_list = random.sample(range(data_len), k=int(data_len * self.impact_model_ratio))
        # The input feature is (normalized mini model prediction, predicted global resource util, the predicted
        # resource util on the same core that the opunit group runs)
        # The output target is the ratio between the actual resource util (including the elapsed time) and the
        # normalized mini model prediction
        for idx in tqdm.tqdm(sample_list, desc="Construct data for the {} model".format(model_name)):
            d = impact_data_list[idx]
            mini_model_y_pred.append(d.target_grouped_op_unit_data.y_pred)
            predicted_elapsed_us = mini_model_y_pred[-1][data_info.TARGET_CSV_INDEX[Target.ELAPSED_US]]
            predicted_resource_util = None
            if model_name == "impact":
                predicted_resource_util = d.resource_data.y_pred
            if model_name == "direct":
                predicted_resource_util = d.resource_data.x
            x.append(np.concatenate((mini_model_y_pred[-1] / predicted_elapsed_us, predicted_resource_util,
                                     d.resource_util_same_core_x)))
            # x.append(np.concatenate((mini_model_y_pred / predicted_elapsed_us, predicted_resource_util)))
            raw_y.append(d.target_grouped_op_unit_data.y)
            y.append(raw_y[-1] / (mini_model_y_pred[-1] + 1e-6))

        # Training
        metrics_path = "{}/global_{}_model_metrics.csv".format(self.model_results_path, model_name)
        prediction_path = "{}/global_{}_model_prediction.csv".format(self.model_results_path, model_name)
        x = np.array(x)
        y = np.array(y)
        trained_model, test_indices = _global_model_training_process(x, y, self.ml_models, self.test_ratio,
                                                                     metrics_path, prediction_path)

        # Calculate the accumulated ratio error
        mini_model_y_pred = np.array(mini_model_y_pred)[test_indices]
        y_pred = trained_model.predict(x)[test_indices]
        raw_y_pred = mini_model_y_pred * (y_pred + 1e-6)
        raw_y = np.array(raw_y)[test_indices]
        accumulated_raw_y = np.sum(raw_y, axis=0)
        accumulated_raw_y_pred = np.sum(raw_y_pred, axis=0)
        original_ratio_error = np.average(np.abs(raw_y - mini_model_y_pred) / (raw_y + 1e-6), axis=0)
        accumulated_percentage_error = np.abs(accumulated_raw_y - accumulated_raw_y_pred) / (accumulated_raw_y + 1)
        original_accumulated_percentage_error = np.abs(accumulated_raw_y - np.sum(mini_model_y_pred, axis=0)) / (
                accumulated_raw_y + 1)

        logging.info('Original Ratio Error: {}'.format(original_ratio_error))
        logging.info('Original Accumulated Ratio Error: {}'.format(original_accumulated_percentage_error))
        logging.info('Accumulated Ratio Error: {}'.format(accumulated_percentage_error))

        return trained_model


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Global Trainer')
    aparser.add_argument('--input_path', default='global_runner_input',
                         help='Input file path for the global runners')
    aparser.add_argument('--model_results_path', default='global_model_results',
                         help='Prediction results of the mini models')
    aparser.add_argument('--save_path', default='trained_model', help='Path to save the trained models')
    aparser.add_argument('--mini_model_file', default='trained_nogen_model/mini_model_map.pickle',
                         help='File of the saved mini models')
    aparser.add_argument('--ml_models', nargs='*', type=str, default=["nn"],
                         help='ML models for the mini trainer to evaluate')
    aparser.add_argument('--test_ratio', type=float, default=0.2, help='Test data split ratio')
    aparser.add_argument('--impact_model_ratio', type=float, default=1,
                         help='Sample ratio to train the global impact model')
    aparser.add_argument('--warmup_period', type=float, default=1, help='OLTPBench warmup period')
    aparser.add_argument('--simulate_cache', default=False, help='Should simulate cache at 0.4')
    aparser.add_argument('--tpcc_hack', default=False, help='Should do feature correction for TPCC')
    aparser.add_argument('--log', default='info', help='The logging level')
    args = aparser.parse_args()

    logging_util.init_logging(args.log)

    logging.info("Global trainer starts.")

    with open(args.mini_model_file, 'rb') as pickle_file:
        model_map = pickle.load(pickle_file)
    trainer = GlobalTrainer(args.input_path, args.model_results_path, args.ml_models, args.test_ratio,
                            args.impact_model_ratio, model_map, args.warmup_period, args.simulate_cache, args.tpcc_hack)
    resource_model, impact_model, direct_model = trainer.train()
    with open(args.save_path + '/global_resource_model.pickle', 'wb') as file:
        pickle.dump(resource_model, file)
    with open(args.save_path + '/global_impact_model.pickle', 'wb') as file:
        pickle.dump(impact_model, file)
    with open(args.save_path + '/global_direct_model.pickle', 'wb') as file:
        pickle.dump(direct_model, file)
