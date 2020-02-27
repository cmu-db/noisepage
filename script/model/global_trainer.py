#!/usr/bin/env python3

import glob
import os
import numpy as np
import argparse
import pickle
import copy

import grouped_op_unit_data
import data_info
import io_util
from type import OpUnit

np.set_printoptions(precision=4)
np.set_printoptions(edgeitems=10)
np.set_printoptions(suppress=True)



class GlobalTrainer:
    """
    Trainer for the mini models
    """

    def __init__(self, input_path, model_metrics_path, ml_models, test_ratio, mini_model_map):
        self.input_path = input_path
        self.model_metrics_path = model_metrics_path
        self.ml_models = ml_models
        self.test_ratio = test_ratio
        self.mini_model_map = mini_model_map

    def train(self):
        """Train the mini-models

        :return: the map of the trained models
        """

        data_list = self._get_data_list()
        self._predict_global_data(data_list)
        self._construct_global_model_data(data_list)

    def _construct_global_model_data(self, data_list):
        """Construct the GlobalModelData used for the global model training

        :param data_list: The list of the GlobalModelData objects
        """
        start_time_idx = [i[0] for i in sorted(enumerate(data_list), key=lambda x: x[1].start_time)]
        # The concurrent running opunit groups that that has already started
        started_data_list = []
        for i in start_time_idx:
            data = data_list[start_time_idx[i]]

            # First find the overlap between the current opunit group and the previously started groups
            started_data_list.append(data)
            concurrent_data_list = []
            for started_data in started_data_list:
                if started_data.end_time <= data.start_time:
                    # The entered_data overlaps with the current opunit group
                    concurrent_data_list.append(started_data)
            started_data_list = copy.deepcopy(concurrent_data_list)

            # Then find the overlap with the opunit groups started later
            j = i + 1
            while data_list[start_time_idx[j]].start_time <= data.end_time and j < len(start_time_idx):
                concurrent_data_list.append(data_list[start_time_idx[j]])
            concurrent_ratio_list = []

    def _get_data_list(self):
        """Get the list of all the operating units (or groups of operating units) stored in GlobalData objects
        :return: the list of all the operating units (or groups of operating units) stored in GlobalData objects
        """
        data_list = []

        # First get the data for all mini runners
        for filename in glob.glob(os.path.join(self.input_path, '*.csv')):
            print(filename)
            data_list += grouped_op_unit_data.get_grouped_op_unit_data(filename)

        return data_list

    def _predict_global_data(self, data_list):
        """Use the mini-runner to predict the resource consumptions for all the GlobalData, and record the prediction
        result in place

        :param data_list: The list of the GroupedOpUnitData objects
        """

        prediction_path = "{}/prediction.csv".format(self.model_metrics_path)
        open(prediction_path, 'w').close()
        io_util.write_result(prediction_path, "Pipeline", ["Actual", "Predicted", "Ratio Error"])

        # Have to use a prediction cache when having lots of global data...
        prediction_cache = {}

        # First run a prediction on the global running data with the mini model results
        for i, data in enumerate(data_list):
            if i == 100:
                break
            y = data.y
            print("{} pipeline elapsed time: {}".format(data.name, y[-1]))
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
                print("Predicted {} elapsed time with feature {}: {}".format(opunit_feature[0].name,
                                                                             x[0], y_pred[0, -1]))
                pipeline_y_pred += y_pred[0]

            # Record the predicted
            data.y_pred = pipeline_y_pred
            print("{} pipeline predicted time: {}".format(data.name, pipeline_y_pred[-1]))
            ratio_error = abs(y[-1] - pipeline_y_pred[-1]) / y[-1]
            print("|Actual - Predict| / Actual: {}".format(ratio_error))

            io_util.write_result(prediction_path, data.name + " " + str(x[0][-1]),
                                 [y[-1], pipeline_y_pred[-1], ratio_error])

            print()


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Mini Trainer')
    aparser.add_argument('--input_path', default='global_runner_input', help='Input file path for the global runners')
    aparser.add_argument('--model_metrics_path', default='global_model_metrics',
                         help='Prediction metrics of the mini models')
    aparser.add_argument('--save_path', default='trained_model', help='Path to save the trained models')
    aparser.add_argument('--mini_model_file', default='trained_model/mini_model_map.pickle',
                         help='File of the saved mini models')
    aparser.add_argument('--ml_models', nargs='*', type=str, default=["lr", "rf", "nn"],
                         help='ML models for the mini trainer to evaluate')
    aparser.add_argument('--test_ratio', type=float, default=0.2, help='Test data split ratio')
    args = aparser.parse_args()

    with open(args.mini_model_file, 'rb') as pickle_file:
        model_map = pickle.load(pickle_file)
    trainer = GlobalTrainer(args.input_path, args.model_metrics_path, args.ml_models, args.test_ratio,
                            model_map)
    trainer.train()
