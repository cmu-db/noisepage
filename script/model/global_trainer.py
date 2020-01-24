#!/usr/bin/env python3

import glob
import os
import numpy as np
import argparse
import pickle


import global_data
import data_info

np.set_printoptions(precision=4)
np.set_printoptions(edgeitems=10)
np.set_printoptions(suppress=True)


def _get_result_labels():
    labels = []
    for dataset in ["Train", "Test"]:
        for target in data_info.mini_model_target_list:
            labels.append(dataset + " " + target.name)
        labels.append("")

    return labels


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

        data_list = []

        # First get the data for all mini runners
        for filename in glob.glob(os.path.join(self.input_path, '*.csv')):
            print(filename)
            data_list += global_data.get_global_data(filename)

        # First run a prediction on the global running data with the mini model results
        for data in data_list:
            y = data.y
            print("pipeline elapsed time: {}".format(y[-1]))
            for opunit_feature in data.opunit_features:
                opunit_model = self.mini_model_map[opunit_feature[0]]
                x = np.array(opunit_feature[1]).reshape(1, -1)
                y_pred = opunit_model.predict(x)
                print("Predicted {} elapsed time with feature {}: {}".format(opunit_feature[0].name,
                                                                             x[0], y_pred[0, -1]))
            print()


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Mini Trainer')
    aparser.add_argument('--input_path', default='global_runner_input', help='Input file path for the global runners')
    aparser.add_argument('--model_metrics_path', default='global_runner_model_metrics',
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
