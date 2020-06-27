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
    :return: the best model
    """
    global_model = None
    result_writing_util.create_metrics_and_prediction_files(metrics_path, prediction_path)

    x_train, x_test, y_train, y_test = model_selection.train_test_split(x, y, test_size=test_ratio, random_state=0)

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

    return global_model


class GlobalTrainer:
    """
    Trainer for the mini models
    """

    def __init__(self, input_path, model_results_path, ml_models, test_ratio, impact_model_ratio, mini_model_map):
        self.input_path = input_path
        self.model_results_path = model_results_path
        self.ml_models = ml_models
        self.test_ratio = test_ratio
        self.impact_model_ratio = impact_model_ratio
        self.mini_model_map = mini_model_map

    def train(self):
        """Train the mini-models

        :return: the map of the trained models
        """
        resource_data_list, impact_data_list = global_data_constructing_util.get_data(self.input_path,
                                                                                      self.mini_model_map,
                                                                                      self.model_results_path)

        return self._train_global_models(resource_data_list, impact_data_list)

    def _train_global_models(self, resource_data_list, impact_data_list):
        """Train the global models

        :param resource_data_list: list of GlobalResourceData
        :param impact_data_list: list of GlobalImpactData
        :return: (global resource model, global impact model)
        """
        methods = self.ml_models

        # First train the resource prediction model
        # Get the features and labels
        x = np.array([d.x for d in resource_data_list])
        y = np.array([d.y for d in resource_data_list])

        # Training
        metrics_path = "{}/global_resource_model_metrics.csv".format(self.model_results_path)
        prediction_path = "{}/global_resource_model_prediction.csv".format(self.model_results_path)
        global_resource_model = _global_model_training_process(x, y, methods, self.test_ratio, metrics_path,
                                                               prediction_path)

        # Put the prediction global resource util back to the GlobalImpactData
        y_pred = global_resource_model.predict(x)
        for i, data in enumerate(resource_data_list):
            data.y_pred = y_pred[i]

        # Then train the global impact model
        x = []
        y = []
        data_len = len(impact_data_list)
        sample_list = random.sample(range(data_len), k=int(data_len*self.impact_model_ratio))
        # The input feature is (normalized mini model prediction, predicted global resource util, the predicted
        # resource util on the same core that the opunit group runs)
        # The output target is the ratio between the actual resource util (including the elapsed time) and the
        # normalized mini model prediction
        for idx in tqdm.tqdm(sample_list, desc="Construct data for the impact model"):
            d = impact_data_list[idx]
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

        methods = self.ml_models

        # Training
        metrics_path = "{}/global_impact_model_metrics.csv".format(self.model_results_path)
        prediction_path = "{}/global_impact_model_prediction.csv".format(self.model_results_path)
        global_impact_model = _global_model_training_process(np.array(x), np.array(y), methods, self.test_ratio,
                                                             metrics_path, prediction_path)

        return global_resource_model, global_impact_model


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Global Trainer')
    aparser.add_argument('--input_path', default='global_runner_input', help='Input file path for the global runners')
    aparser.add_argument('--model_results_path', default='global_model_results',
                         help='Prediction results of the mini models')
    aparser.add_argument('--save_path', default='trained_model', help='Path to save the trained models')
    aparser.add_argument('--mini_model_file', default='trained_model/mini_model_map.pickle',
                         help='File of the saved mini models')
    aparser.add_argument('--ml_models', nargs='*', type=str, default=["huber"],
                         help='ML models for the mini trainer to evaluate')
    aparser.add_argument('--test_ratio', type=float, default=0.2, help='Test data split ratio')
    aparser.add_argument('--impact_model_ratio', type=float, default=1, help=
                         'Sample ratio to train the global impact model')
    aparser.add_argument('--log', default='info', help='The logging level')
    args = aparser.parse_args()

    logging_util.init_logging(args.log)

    logging.info("Global trainer starts.")

    with open(args.mini_model_file, 'rb') as pickle_file:
        model_map = pickle.load(pickle_file)
    trainer = GlobalTrainer(args.input_path, args.model_results_path, args.ml_models, args.test_ratio,
                            args.impact_model_ratio, model_map)
    resource_model, impact_model = trainer.train()
    with open(args.save_path + '/global_resource_model.pickle', 'wb') as file:
        pickle.dump(resource_model, file)
    with open(args.save_path + '/global_impact_model.pickle', 'wb') as file:
        pickle.dump(impact_model, file)
