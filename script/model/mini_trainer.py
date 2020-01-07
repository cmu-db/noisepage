#!/usr/bin/env python3

import glob
import os
import numpy as np
import argparse

from sklearn.model_selection import train_test_split

from data_util import write_result, get_data_list, get_concurrent_data_list
from model import Model
import data_util

np.set_printoptions(precision=4)
np.set_printoptions(suppress=True)

'''
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
'''

EPS = 1e-6

class MiniTrainer:
    '''
    Trainer for the mini models
    '''

    def __init__(self, input_path, save_path, ml_models, test_ratio):
        self.input_path = input_path
        self.save_path = save_path
        self.ml_models = ml_models
        self.test_ratio = test_ratio

    def train(self):
        '''
        Train the mini-models
        :return: the map of the trained models
        '''

        data_map = {}

        for filename in glob.glob(os.path.join(self.input_path, '*.csv')):
            print(filename)
            if "txn" not in filename and "execution" not in filename:
                data = data_util.get_mini_runner_data(filename)
                data_map[data.opunit] = data
            if "execution" in filename:
                data_list = data_util.get_execution_mini_runner_data(filename)
                for data in data_list:
                    data_map[data.opunit] = data


        '''
        data_list = get_concurrent_data_list(input_path)

        pred_map = {}
        target_data_list = []

        for d in data_list:
            if (d.symbol == 'counterfeature' or d.symbol == 'rawandcounterfeature') and d.target == 'ratio':
                target_data_list.append(d)

            x = d.data[:, :-1]
            y = d.data[:, -1]

            x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_ratio, random_state=0)

            result_path = "{}/{}_{}_result.csv".format(output_path, d.symbol, d.target)

            open(result_path, 'w').close()
            labels = ["Train", "Test"]
            write_result(result_path, "Method", labels)

            min_pe = 1
            key = (d.symbol, d.target)
            for method in methods:
                regressor = Model(method)
                regressor.train(x_train, y_train)

                print("{} {} {}".format(d.symbol, d.target, method))
                results = []
                evaluate_data = [(x_train, y_train), (x_test, y_test)]
                for i, data in enumerate(evaluate_data):
                    evaluate_x = data[0]
                    evaluate_y = data[1]

                    y_pred = regressor.predict(evaluate_x)
                    pe = np.average(np.abs(evaluate_y - y_pred) / (evaluate_y + EPS))
                    results.append(pe)

                    label = labels[i]
                    print('{} Percenrage Error: {}'.format(label, pe))

                    if i == 1 and pe < min_pe:
                        min_pe = pe
                        pred_map[key] = y_pred

                write_result(result_path, method, results)

                print()

        for d in target_data_list:
            result_path = "{}/hierarchical_{}_{}_result.csv".format(output_path, d.symbol, d.target)
            open(result_path, 'w').close()
            labels = ["Train", "Test"]
            write_result(result_path, "Method", labels)

            x = d.data[:, :-1]
            y = d.data[:, -1]

            x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_ratio, random_state=0)
            x_test[:, -5] = pred_map[("rawfeature", "cpu")]
            x_test[:, -4] = pred_map[("rawfeature", "ref")]
            x_test[:, -3] = pred_map[("rawfeature", "miss")]
            x_test[:, -2] = pred_map[("rawfeature", "instr")]

            for method in methods:
                regressor = Model(method)
                regressor.train(x_train, y_train)

                print("hierarchical {} {} {}".format(d.symbol, d.target, method))
                results = []
                evaluate_data = [(x_train, y_train), (x_test, y_test)]
                for i, data in enumerate(evaluate_data):
                    evaluate_x = data[0]
                    evaluate_y = data[1]

                    y_pred = regressor.predict(evaluate_x)
                    pe = np.average(np.abs(evaluate_y - y_pred) / (evaluate_y + EPS))
                    results.append(pe)

                    label = labels[i]
                    print('{} Percenrage Error: {}'.format(label, pe))

                write_result(result_path, method, results)
                print()
        '''

    def save(self):
        '''
        Save the trained models to the save_path
        :return:
        '''


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Mini Trainer')
    aparser.add_argument('--input_path', default='mini_runner_input', help='Input file path for the mini runners')
    aparser.add_argument('--save_path', default='mini_model', help='Path to save the mini models')
    aparser.add_argument('--ml_models', nargs='*', type=str, default=["lr", "rf", "xgb", "nn"],
                         help='ML models for the mini trainer to evaluate')
    aparser.add_argument('--test_ratio', type=float, default=0.2, help='Test data split ratio')
    args = aparser.parse_args()

    trainer = MiniTrainer(args.input_path, args.save_path, args.ml_models, args.test_ratio)
    trainer.train()

