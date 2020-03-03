#!/usr/bin/env python3

import glob
import os
import numpy as np
import argparse
import pickle
import logging

from sklearn import model_selection

import grouped_op_unit_data
import data_info
import io_util
import global_model_data
import model
import training_util
import logging_util
from type import OpUnit, Target

np.set_printoptions(precision=4)
np.set_printoptions(edgeitems=10)
np.set_printoptions(suppress=True)


def _calculate_range_overlap(start_timel, end_timel, start_timer, end_timer):
    return min(end_timel, end_timer) - max(start_timel, start_timer) + 1


def _get_global_resource_util(grouped_opunit, concurrent_data_list, prediction_path):
    """Get the input feature and the target output for the global resource utilization metrics during the time range
    of a GroupedOpUnitData

    The calculation is adjusted by the overlapping ratio between the opunit groups and the time range.

    :param grouped_opunit: the data to calculate the global resource utilization for
    :param concurrent_data_list: the concurrent running opunit groups
    :return: (the input feature, the resource utilization on the other logical core of the same physical core,
    the output resource targets)
    """
    start_time = grouped_opunit.start_time
    end_time = grouped_opunit.end_time
    elapsed_us = end_time - start_time + 1

    # The adjusted resource metrics per logical core.
    # FIXME: Assuming that the machine has 20 logical cores for now (dev4). We may want to create a hardware_info
    #  module eventually
    adjusted_x_list = [0] * 20
    adjusted_y = 0
    logging.debug(concurrent_data_list)
    logging.debug("{} {}".format(start_time, end_time))

    for data in concurrent_data_list:
        ratio = _calculate_range_overlap(start_time, end_time, data.start_time, data.end_time) / (data.end_time -
                                                                                                  data.start_time + 1)
        logging.debug("{} {} {}".format(data.start_time, data.end_time, ratio))
        adjusted_y += data.y * ratio
        cpu_id = data.cpu_id
        if cpu_id > 10:
            cpu_id -= 10
        adjusted_x_list[cpu_id] += data.y_pred * ratio

    # change the number to per time unit (us) utilization
    for x in adjusted_x_list:
        x /= elapsed_us
    adjusted_y /= elapsed_us

    sum_adjusted_x = np.sum(adjusted_x_list, axis=0)
    std_adjusted_x = np.std(adjusted_x_list, axis=0)
    cpu_id = grouped_opunit.cpu_id
    same_core_adjusted_x = adjusted_x_list[cpu_id - 10 if cpu_id > 10 else cpu_id]

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
    training_util.create_metrics_and_prediction_files(metrics_path, prediction_path)

    x_train, x_test, y_train, y_test = model_selection.train_test_split(x, y, test_size=test_ratio, random_state=0)

    min_percentage_error = 1
    pred_results = None
    elapsed_us_index = data_info.target_csv_index[Target.ELAPSED_US]

    for method in methods:
        # Train the model
        logging.info("Training the global model with {}".format(method))
        regressor = model.Model(method, log_transform=False)
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

            logging.info('{} Percentage Error: {}'.format(train_test_label[i], percentage_error))

            # Record the model with the lowest elapsed time prediction (since that might be the most
            # important prediction)
            if i == 1 and percentage_error[elapsed_us_index] < min_percentage_error:
                min_percentage_error = percentage_error[elapsed_us_index]
                global_model = regressor
                pred_results = (evaluate_x, evaluate_y, y_pred)

        io_util.write_csv_result(metrics_path, method, results)

        logging.info("")

    # Record the best prediction results on the test data
    training_util.record_predictions(pred_results, prediction_path)

    return global_model


class GlobalTrainer:
    """
    Trainer for the mini models
    """

    def __init__(self, input_path, model_results_path, ml_models, test_ratio, mini_model_map):
        self.input_path = input_path
        self.model_results_path = model_results_path
        self.ml_models = ml_models
        self.test_ratio = test_ratio
        self.mini_model_map = mini_model_map

    def train(self):
        """Train the mini-models

        :return: the map of the trained models
        """

        data_list = self._get_data_list()
        self._predict_grouped_opunit_data(data_list)
        global_model_data_list = self._construct_global_model_data(data_list)
        return self._train_global_models(global_model_data_list)

    def _train_global_models(self, global_model_data_list):
        """Train the global models

        :param global_model_data_list: list of the GlobalModelData
        :return: (global resource model, global impact model)
        """
        methods = self.ml_models

        # First train the resource prediction model
        # Get the features and labels
        x = np.array([d.global_resource_util_x for d in global_model_data_list])
        y = np.array([d.global_resource_util_y for d in global_model_data_list])

        # Training
        metrics_path = "{}/global_resource_model_metrics.csv".format(self.model_results_path)
        prediction_path = "{}/global_resource_model_prediction.csv".format(self.model_results_path)
        global_resource_model = _global_model_training_process(x, y, methods, self.test_ratio, metrics_path,
                                                               prediction_path)

        # Put the prediction global resource util back to the GlobalModelData
        y_pred = global_resource_model.predict(x)
        for i, data in enumerate(global_model_data_list):
            data.global_resource_util_y_pred = y_pred[i]

        # Then train the global impact model
        x = []
        y = []
        # The input feature is (normalized mini model prediction, predicted global resource util, the predicted
        # resource util on the same core that the opunit group runs)
        # The output target is the ratio between the actual resource util (including the elapsed time) and the
        # normalized mini model prediction
        for d in global_model_data_list:
            mini_model_y_pred = d.target_grouped_op_unit_data.y_pred
            predicted_elapsed_us = mini_model_y_pred[data_info.target_csv_index[Target.ELAPSED_US]]
            x.append(np.concatenate((mini_model_y_pred / predicted_elapsed_us, d.global_resource_util_y_pred,
                                     d.global_resource_util_same_core_x)))
            # x.append(np.concatenate((mini_model_y_pred / predicted_elapsed_us, d.global_resource_util_y_pred)))
            y.append(d.target_grouped_op_unit_data.y / (d.target_grouped_op_unit_data.y_pred + 1e-6))
        methods = self.ml_models

        # First train the resource prediction model
        metrics_path = "{}/global_impact_model_metrics.csv".format(self.model_results_path)
        prediction_path = "{}/global_impact_model_prediction.csv".format(self.model_results_path)
        global_impact_model = _global_model_training_process(np.array(x), np.array(y), methods, self.test_ratio,
                                                             metrics_path, prediction_path)

        return global_resource_model, global_impact_model

    def _construct_global_model_data(self, data_list):
        """Construct the GlobalModelData used for the global model training

        :param data_list: The list of the GlobalModelData objects
        """
        prediction_path = "{}/global_resource_data.csv".format(self.model_results_path)
        io_util.create_csv_file(prediction_path, ["Elapsed us", "# Concurrent OpUnit Groups"])

        global_model_data_list = []

        start_time_idx = [i[0] for i in sorted(enumerate(data_list), key=lambda a: a[1].start_time)]
        # The concurrent running opunit groups that that has already started
        started_data_list = []
        for i, idx in enumerate(start_time_idx):
            data = data_list[idx]

            # First find the overlap between the current opunit group and the previously started groups
            started_data_list.append(data)
            concurrent_data_list = []
            for started_data in started_data_list:
                if started_data.end_time >= data.start_time:
                    # The entered_data overlaps with the current opunit group
                    concurrent_data_list.append(started_data)
            started_data_list = concurrent_data_list.copy()

            # Then find the overlap with the opunit groups started later
            j = i + 1
            while j < len(start_time_idx) and data_list[start_time_idx[j]].start_time <= data.end_time:
                concurrent_data_list.append(data_list[start_time_idx[j]])
                j += 1

            x, same_core_x, y = _get_global_resource_util(data, concurrent_data_list, prediction_path)
            global_model_data_list.append(global_model_data.GlobalModelData(data, concurrent_data_list, x,
                                                                            same_core_x, y))

        return global_model_data_list

    def _get_data_list(self):
        """Get the list of all the operating units (or groups of operating units) stored in GlobalData objects
        :return: the list of all the operating units (or groups of operating units) stored in GlobalData objects
        """
        data_list = []

        # First get the data for all mini runners
        for filename in glob.glob(os.path.join(self.input_path, '*.csv')):
            print(filename)
            data_list += grouped_op_unit_data.get_grouped_op_unit_data(filename)
            # break

        return data_list

    def _predict_grouped_opunit_data(self, data_list):
        """Use the mini-runner to predict the resource consumptions for all the GlobalData, and record the prediction
        result in place

        :param data_list: The list of the GroupedOpUnitData objects
        """

        prediction_path = "{}/grouped_opunit_prediction.csv".format(self.model_results_path)
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
                opunit_model = self.mini_model_map[opunit]
                x = np.array(opunit_feature[1]).reshape(1, -1)
                key = (opunit, x.tobytes())
                if key not in prediction_cache:
                    y_pred = opunit_model.predict(x)
                    prediction_cache[key] = y_pred
                    # subtract scan from certain double-counted opunits
                    if opunit in data_info.scan_subtract_opunits:
                        scan_y_pred = self.mini_model_map[OpUnit.SCAN].predict(x)
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


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Mini Trainer')
    aparser.add_argument('--input_path', default='global_runner_input', help='Input file path for the global runners')
    aparser.add_argument('--model_results_path', default='global_model_results',
                         help='Prediction results of the mini models')
    aparser.add_argument('--save_path', default='trained_model', help='Path to save the trained models')
    aparser.add_argument('--mini_model_file', default='trained_model/mini_model_map.pickle',
                         help='File of the saved mini models')
    aparser.add_argument('--ml_models', nargs='*', type=str, default=["lr", "rf", "gbm", "nn"],
                         help='ML models for the mini trainer to evaluate')
    aparser.add_argument('--test_ratio', type=float, default=0.2, help='Test data split ratio')
    aparser.add_argument('--log', default='info', help='The logging level')
    args = aparser.parse_args()

    logging_util.init_logging(args.log)

    with open(args.mini_model_file, 'rb') as pickle_file:
        model_map = pickle.load(pickle_file)
    trainer = GlobalTrainer(args.input_path, args.model_results_path, args.ml_models, args.test_ratio,
                            model_map)
    resource_model, impact_model = trainer.train()
    #with open(args.save_path + '/global_resource_model.pickle', 'wb') as file:
    #    pickle.dump(resource_model, file)
    #with open(args.save_path + '/global_impact_model.pickle', 'wb') as file:
    #    pickle.dump(impact_model, file)
