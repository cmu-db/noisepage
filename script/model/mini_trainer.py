#!/usr/bin/env python3

import glob
import os
import numpy as np
import argparse
import pickle

from sklearn import model_selection

import model
import opunit_data
import data_info
import data_transform

from type import Target

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


class MiniTrainer:
    """
    Trainer for the mini models
    """

    def __init__(self, input_path, model_metrics_path, ml_models, test_ratio):
        self.input_path = input_path
        self.model_metrics_path = model_metrics_path
        self.ml_models = ml_models
        self.test_ratio = test_ratio

    def train(self):
        """Train the mini-models

        :return: the map of the trained models
        """

        data_list = []

        # First get the data for all mini runners
        for filename in glob.glob(os.path.join(self.input_path, '*.csv')):
            print(filename)
            data_list += opunit_data.get_mini_runner_data(filename)

        labels = _get_result_labels()

        model_map = {}
        # train the models for all the operating units
        for data in data_list:
            x_train, x_test, y_train, y_test = model_selection.train_test_split(data.x, data.y,
                                                                                test_size=self.test_ratio,
                                                                                random_state=0)

            # Write the first header rwo to the result file
            result_path = "{}/{}.csv".format(self.model_metrics_path, data.opunit.name.lower())
            prediction_path = "{}/{}_prediction.csv".format(self.model_metrics_path, data.opunit.name.lower())
            open(result_path, 'w').close()
            open(prediction_path, 'w').close()
            opunit_data.write_result(result_path, "Method", labels)

            methods = self.ml_models
            # Only use linear regression for the arithmetic operating units
            if data.opunit in data_info.arithmetic_opunits:
                methods = ["lr"]

            # Also test the prediction with the target transformer (if specified for the operating unit)
            transformers = [None]
            modeling_transformer = data_transform.opunit_modeling_trainsformer_map[data.opunit]
            if modeling_transformer is not None:
                transformers.append(modeling_transformer)

            min_percentage_error = 1
            pred_results = None
            elapsed_us_index = data_info.target_csv_index[Target.ELAPSED_US]

            for transformer in transformers:
                for method in methods:
                    # Train the model
                    print("{} {}".format(data.opunit.name, method))
                    regressor = model.Model(method, modeling_transformer=transformer)
                    regressor.train(x_train, y_train)

                    # Evaluate on both the training and test set
                    results = []
                    evaluate_data = [(x_train, y_train), (x_test, y_test)]
                    train_test_label = ["Train", "Test"]
                    for i, d in enumerate(evaluate_data):
                        evaluate_x = d[0]
                        evaluate_y = d[1]

                        y_pred = regressor.predict(evaluate_x)
                        print("x shape: ", evaluate_x.shape)
                        print("y shape: ", y_pred.shape)
                        percentage_error = np.average(np.abs(evaluate_y - y_pred) / (evaluate_y + 1), axis=0)
                        results += list(percentage_error) + [""]

                        print('{} Percentage Error: {}'.format(train_test_label[i], percentage_error))

                        # Record the model with the lowest elapsed time prediction (since that might be the most
                        # important prediction)
                        if (i == 1 and percentage_error[elapsed_us_index] < min_percentage_error and transformer ==
                                transformers[-1]):
                            min_percentage_error = percentage_error[elapsed_us_index]
                            model_map[data.opunit] = regressor
                            pred_results = (evaluate_x, evaluate_y, y_pred)

                    # Dump the prediction results
                    transform = " "
                    if transformer is not None:
                        transform = " transform"
                    opunit_data.write_result(result_path, method + transform, results)

                    print()

                opunit_data.write_result(result_path, "", [])

            # Record the best prediction results on the test data
            num_data = pred_results[0].shape[0]
            for i in range(num_data):
                result_list = (list(pred_results[0][i]) + [""] + list(pred_results[1][i]) + [""]
                               + list(pred_results[2][i]))
                opunit_data.write_result(prediction_path, "", result_list)

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

        return model_map


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Mini Trainer')
    aparser.add_argument('--input_path', default='mini_runner_input', help='Input file path for the mini runners')
    aparser.add_argument('--model_metrics_path', default='mini_runner_model_metrics',
                         help='Prediction metrics of the mini models')
    aparser.add_argument('--save_path', default='trained_model', help='Path to save the mini models')
    aparser.add_argument('--ml_models', nargs='*', type=str, default=["lr", "rf", "nn"],
                         help='ML models for the mini trainer to evaluate')
    aparser.add_argument('--test_ratio', type=float, default=0.2, help='Test data split ratio')
    args = aparser.parse_args()

    trainer = MiniTrainer(args.input_path, args.model_metrics_path, args.ml_models, args.test_ratio)
    trained_model_map = trainer.train()
    if args.save_path is not None:
        with open(args.save_path + '/mini_model_map.pickle', 'wb') as file:
            pickle.dump(trained_model_map, file)
