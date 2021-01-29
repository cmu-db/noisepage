#!/usr/bin/env python3

import numpy as np
import argparse
import pickle
import logging
import tqdm
import random
from sklearn import model_selection

import model
import global_model_config
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
    result_writing_util.create_metrics_and_prediction_files(metrics_path, prediction_path, False)
    n_samples = x.shape[0]
    indices = np.arange(n_samples)

    x_train, x_test, y_train, y_test, indices_train, indices_test = model_selection.train_test_split(
        x, y, indices, test_size=test_ratio, random_state=0)

    min_percentage_error = 1
    pred_results = None
    elapsed_us_index = data_info.instance.target_csv_index[Target.ELAPSED_US]

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
    Trainer for the global models
    """

    def __init__(self, input_path, model_results_path, ml_models, test_ratio, impact_model_ratio, mini_model_map,
                 warmup_period, use_query_predict_cache, add_noise, predict_ou_only, ee_sample_interval,
                 txn_sample_interval, network_sample_interval):
        self.input_path = input_path
        self.model_results_path = model_results_path
        self.ml_models = ml_models
        self.test_ratio = test_ratio
        self.impact_model_ratio = impact_model_ratio
        self.mini_model_map = mini_model_map
        self.warmup_period = warmup_period
        self.use_query_predict_cache = use_query_predict_cache
        self.add_noise = add_noise
        self.predict_ou_only = predict_ou_only
        self.ee_sample_interval = ee_sample_interval
        self.txn_sample_interval = txn_sample_interval
        self.network_sample_interval = network_sample_interval

        self.resource_data_list = None
        self.impact_data_list = None

    def predict_ou_data(self):
        """Generate grouped OU data with prediction
        """

        data_lists = global_data_constructing_util.get_data(self.input_path,
                                                            self.mini_model_map,
                                                            self.model_results_path,
                                                            self.warmup_period,
                                                            self.use_query_predict_cache,
                                                            self.add_noise,
                                                            self.predict_ou_only,
                                                            self.ee_sample_interval,
                                                            self.txn_sample_interval,
                                                            self.network_sample_interval)

        self.resource_data_list = data_lists[0]
        self.impact_data_list = data_lists[1]

    def train(self):
        """Train the global models (needs to call predict_ou_data first to predict the grouped OU data)

        :return: (global_resource_model, global_impact_model, global_direct_model)
        """
        # First train the resource prediction model
        # Get the features and labels
        x = np.array([d.x for d in self.resource_data_list])
        y = np.array([d.y for d in self.resource_data_list])

        # Training
        metrics_path = "{}/global_resource_model_metrics.csv".format(self.model_results_path)
        prediction_path = "{}/global_resource_model_prediction.csv".format(self.model_results_path)
        global_resource_model, _ = _global_model_training_process(x, y, self.ml_models, self.test_ratio, metrics_path,
                                                                  prediction_path)

        # Put the prediction global resource util back to the GlobalImpactData
        y_pred = global_resource_model.predict(x)
        for i, data in enumerate(self.resource_data_list):
            data.y_pred = y_pred[i]

        global_impact_model = self._train_model_with_derived_data(self.impact_data_list, "impact")

        global_direct_model = self._train_model_with_derived_data(self.impact_data_list, "direct")

        return global_resource_model, global_impact_model, global_direct_model

    def _train_model_with_derived_data(self, impact_data_list, model_name):
        # Then train the global impact model
        x = []
        y = []
        mini_model_y_pred = []  # The labels directly predicted from the mini models
        raw_y = []  # The actual labels
        data_len = len(impact_data_list)
        sample_list = random.sample(range(data_len), k=int(data_len * self.impact_model_ratio))
        epsilon = global_model_config.RATIO_DIVISION_EPSILON
        # The input feature is (normalized mini model prediction, predicted global resource util, the predicted
        # resource util on the same core that the opunit group runs)
        # The output target is the ratio between the actual resource util (including the elapsed time) and the
        # normalized mini model prediction
        for idx in tqdm.tqdm(sample_list, desc="Construct data for the {} model".format(model_name)):
            d = impact_data_list[idx]
            mini_model_y_pred.append(d.target_grouped_op_unit_data.y_pred)
            predicted_elapsed_us = mini_model_y_pred[-1][data_info.instance.target_csv_index[Target.ELAPSED_US]]
            predicted_resource_util = None
            if model_name == "impact":
                predicted_resource_util = d.get_y_pred().copy()
            if model_name == "direct":
                predicted_resource_util = d.x.copy()
            # Remove the OU group itself from the total resource data
            self_resource = (mini_model_y_pred[-1] * max(1, d.target_grouped_op_unit_data.concurrency) /
                             len(d.resource_data_list) / global_model_config.INTERVAL_SIZE)
            predicted_resource_util[:mini_model_y_pred[-1].shape[0]] -= self_resource
            predicted_resource_util[predicted_resource_util < 0] = 0
            x.append(np.concatenate((mini_model_y_pred[-1] / predicted_elapsed_us,
                                     predicted_resource_util,
                                     d.resource_util_same_core_x)))
            # x.append(np.concatenate((mini_model_y_pred[-1] / predicted_elapsed_us, predicted_resource_util)))
            raw_y.append(d.target_grouped_op_unit_data.y)
            y.append(raw_y[-1] / (mini_model_y_pred[-1] + epsilon))
            # Do not adjust memory consumption since it shouldn't change
            y[-1][data_info.instance.target_csv_index[Target.MEMORY_B]] = 1

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
        raw_y_pred = (mini_model_y_pred + epsilon) * y_pred
        raw_y = np.array(raw_y)[test_indices]
        accumulated_raw_y = np.sum(raw_y, axis=0)
        accumulated_raw_y_pred = np.sum(raw_y_pred, axis=0)
        original_ratio_error = np.average(np.abs(raw_y - mini_model_y_pred) / (raw_y + epsilon), axis=0)
        ratio_error = np.average(np.abs(raw_y - raw_y_pred) / (raw_y + epsilon), axis=0)
        accumulated_percentage_error = (np.abs(accumulated_raw_y - accumulated_raw_y_pred) /
                                        (accumulated_raw_y + epsilon))
        original_accumulated_percentage_error = np.abs(accumulated_raw_y - np.sum(mini_model_y_pred, axis=0)) / (
                accumulated_raw_y + epsilon)

        logging.info('Original Ratio Error: {}'.format(original_ratio_error))
        logging.info('Ratio Error: {}'.format(ratio_error))
        logging.info('Original Accumulated Ratio Error: {}'.format(original_accumulated_percentage_error))
        logging.info('Accumulated Ratio Error: {}'.format(accumulated_percentage_error))

        return trained_model


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Global Trainer')
    aparser.add_argument('--input_path', default='global_runner_input_tpcc',
                         help='Input file path for the global runners')
    aparser.add_argument('--model_results_path', default='global_model_results_tpcc',
                         help='Prediction results of the mini models')
    aparser.add_argument('--save_path', default='trained_model', help='Path to save the trained models')
    aparser.add_argument('--mini_model_file', default='trained_model/mini_model_map.pickle',
                         help='File of the saved mini models')
    aparser.add_argument('--ml_models', nargs='*', type=str, default=["nn"],
                         help='ML models for the mini trainer to evaluate')
    aparser.add_argument('--test_ratio', type=float, default=0.2, help='Test data split ratio')
    aparser.add_argument('--impact_model_ratio', type=float, default=0.1,
                         help='Sample ratio to train the global impact model')
    aparser.add_argument('--warmup_period', type=float, default=3, help='OLTPBench warmup period')
    aparser.add_argument('--use_query_predict_cache', action='store_true',
                         help='Cache the prediction result based on the query to accelerate')
    aparser.add_argument('--add_noise', action='store_true', help='Add noise to the cardinality estimations')
    aparser.add_argument('--predict_ou_only', action='store_true', help='Only predict the OU data (no training)')
    aparser.add_argument('--ee_sample_interval', type=int, default=49,
                         help='Sampling interval for the execution engine OUs')
    aparser.add_argument('--txn_sample_interval', type=int, default=49,
                         help='Sampling interval for the transaction OUs')
    aparser.add_argument('--network_sample_interval', type=int, default=49,
                         help='Sampling interval for the network OUs')
    aparser.add_argument('--log', default='info', help='The logging level')
    args = aparser.parse_args()

    logging_util.init_logging(args.log)

    logging.info("Global trainer starts.")

    with open(args.mini_model_file, 'rb') as pickle_file:
        model_map, data_info.instance = pickle.load(pickle_file)
    trainer = GlobalTrainer(args.input_path, args.model_results_path, args.ml_models, args.test_ratio,
                            args.impact_model_ratio, model_map, args.warmup_period, args.use_query_predict_cache,
                            args.add_noise, args.predict_ou_only, args.ee_sample_interval, args.txn_sample_interval,
                            args.network_sample_interval)
    trainer.predict_ou_data()
    if not args.predict_ou_only:
        resource_model, impact_model, direct_model = trainer.train()
        with open(args.save_path + '/global_resource_model.pickle', 'wb') as file:
            pickle.dump(resource_model, file)
        with open(args.save_path + '/global_impact_model.pickle', 'wb') as file:
            pickle.dump(impact_model, file)
        with open(args.save_path + '/global_direct_model.pickle', 'wb') as file:
            pickle.dump(direct_model, file)
