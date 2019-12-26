#!/usr/bin/env python3

import numpy as np
import argparse

from sklearn.model_selection import train_test_split

from data_util import write_result, get_data_list, get_concurrent_data_list
from model import Model

np.set_printoptions(precision=4)
np.set_printoptions(suppress=True)

'''
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
'''

EPS = 1e-6

def Main(args):
    #input_path = "./aggregate.csv"
    input_path = "./19-11-24-concurrent/data.csv"
    output_path = './19-11-24-concurrent/'
    test_ratio = 0.2
    methods = ['lr', 'rf', 'xgb', 'nn']

    #data_list = GetDataList(input_path)
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



# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Contention Modeling')
    aparser.add_argument('--method', default='lr', help='ML method')
    aparser.add_argument('--input_path', help='Input file path')
    aparser.add_argument('--test_path', help='Test file path (optional)')
    aparser.add_argument('--output_path', help='Output file path')
    aparser.add_argument('--agg_output_path', help='The aggregate output file path')
    aparser.add_argument('--feature_size', type=int, help='Size of the feature vector')
    aparser.add_argument('--log_norm', type=int, help='Whether to normalize the data to the log scale (1 yes, 0 no)')
    args = aparser.parse_args()
    
    Main(args)
