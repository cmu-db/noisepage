import glob
import os
import numpy as np
import argparse
import pickle
import logging

from sklearn import model_selection

from . import model
from .util import io_util, logging_util
from .data import opunit_data
from .info import data_info
from .training_util import data_transforming_util, result_writing_util
from .type import Target

np.set_printoptions(precision=4)
np.set_printoptions(edgeitems=10)
np.set_printoptions(suppress=True)


class OUModelTrainer:
    """
    Trainer for the ou models
    """

    def __init__(self, input_path, model_metrics_path, ml_models, test_ratio, trim, expose_all, txn_sample_rate):
        self.input_path = input_path
        self.model_metrics_path = model_metrics_path
        self.ml_models = ml_models
        self.test_ratio = test_ratio
        self.model_map = {}
        self.stats_map = {}
        self.trim = trim
        self.expose_all = expose_all
        self.txn_sample_rate = txn_sample_rate

    def get_model_map(self):
        return self.model_map

    def train_specific_model(self, data, y_transformer_idx, method_idx):
        methods = self.ml_models
        method = methods[method_idx]
        label = method if y_transformer_idx == 0 else method + " transform"
        logging.info("Finalizing model {} {}".format(data.opunit.name, label))

        y_transformers = [None, data_transforming_util.OPUNIT_Y_TRANSFORMER_MAP[data.opunit]]
        x_transformer = data_transforming_util.OPUNIT_X_TRANSFORMER_MAP[data.opunit]
        regressor = model.Model(methods[method_idx], y_transformer=y_transformers[y_transformer_idx],
                                x_transformer=x_transformer)
        regressor.train(data.x, data.y)
        self.model_map[data.opunit] = regressor

    def train_data(self, data, summary_file):
        x_train, x_test, y_train, y_test = model_selection.train_test_split(data.x, data.y,
                                                                            test_size=self.test_ratio,
                                                                            random_state=0)

        # Write the first header rwo to the result file
        metrics_path = "{}/{}.csv".format(self.model_metrics_path, data.opunit.name.lower())
        prediction_path = "{}/{}_prediction.csv".format(self.model_metrics_path, data.opunit.name.lower())
        result_writing_util.create_metrics_and_prediction_files(metrics_path, prediction_path, False)

        methods = self.ml_models

        # Test the prediction with/without the target transformer
        y_transformers = [None, data_transforming_util.OPUNIT_Y_TRANSFORMER_MAP[data.opunit]]
        # modeling_transformer = data_transforming_util.OPUNIT_MODELING_TRANSFORMER_MAP[data.opunit]
        # if modeling_transformer is not None:
        #    transformers.append(modeling_transformer)
        x_transformer = data_transforming_util.OPUNIT_X_TRANSFORMER_MAP[data.opunit]

        error_bias = 1
        min_percentage_error = 2
        pred_results = None
        elapsed_us_index = data_info.instance.target_csv_index[Target.ELAPSED_US]
        memory_b_index = data_info.instance.target_csv_index[Target.MEMORY_B]

        best_y_transformer = -1
        best_method = -1
        for i, y_transformer in enumerate(y_transformers):
            for m, method in enumerate(methods):
                # Train the model
                label = method if i == 0 else method + " transform"
                logging.info("{} {}".format(data.opunit.name, label))
                regressor = model.Model(method, y_transformer=y_transformer, x_transformer=x_transformer)
                regressor.train(x_train, y_train)

                # Evaluate on both the training and test set
                results = []
                evaluate_data = [(x_train, y_train), (x_test, y_test)]
                train_test_label = ["Train", "Test"]
                for j, d in enumerate(evaluate_data):
                    evaluate_x = d[0]
                    evaluate_y = d[1]

                    y_pred = regressor.predict(evaluate_x)
                    logging.debug("x shape: {}".format(evaluate_x.shape))
                    logging.debug("y shape: {}".format(y_pred.shape))
                    # In order to avoid the percentage error to explode when the actual label is very small,
                    # we omit the data point with the actual label <= 5 when calculating the percentage error (by
                    # essentially giving the data points with small labels a very small weight)
                    evaluate_threshold = 5
                    weights = np.where(evaluate_y > evaluate_threshold, np.ones(evaluate_y.shape), np.full(evaluate_y.shape, 1e-6))
                    percentage_error = np.average(np.abs(evaluate_y - y_pred) / (evaluate_y + error_bias), axis=0,
                                                  weights=weights)
                    results += list(percentage_error) + [""]

                    logging.info('{} Percentage Error: {}'.format(train_test_label[j], percentage_error))

                    # The default method of deteroung whether a model is better is by comparing the model error
                    # on the elapsed us. For any opunits in MEM_EVALUATE_OPUNITS, we evaluate by comparing the
                    # model error on memory_b.
                    eval_error = percentage_error[elapsed_us_index]
                    if data.opunit in data_info.instance.MEM_EVALUATE_OPUNITS:
                        eval_error = percentage_error[memory_b_index]

                    # Record the model with the lowest elapsed time prediction (since that might be the most
                    # important prediction)
                    # Only use linear regression for the arithmetic operating units
                    if (j == 1 and eval_error < min_percentage_error
                            and y_transformer == y_transformers[-1]
                            and (data.opunit not in data_info.instance.ARITHMETIC_OPUNITS or method == 'lr')):
                        min_percentage_error = eval_error
                        if self.expose_all:
                            best_y_transformer = i
                            best_method = m
                        else:
                            self.model_map[data.opunit] = regressor
                        pred_results = (evaluate_x, y_pred, evaluate_y)

                    if j == 1:
                        io_util.write_csv_result(summary_file, data.opunit.name, [label] + list(percentage_error))

                # Dump the prediction results
                io_util.write_csv_result(metrics_path, label, results)

                logging.info("")

            io_util.write_csv_result(metrics_path, "", [])

        # Record the best prediction results on the test data
        result_writing_util.record_predictions(pred_results, prediction_path)
        return best_y_transformer, best_method

    def train(self):
        """Train the ou-models

        :return: the map of the trained models
        """

        self.model_map = {}

        # Create the results files for the paper
        header = ["OpUnit", "Method"] + [target.name for target in data_info.instance.MINI_MODEL_TARGET_LIST]
        summary_file = "{}/ou_runner.csv".format(self.model_metrics_path)
        io_util.create_csv_file(summary_file, header)

        # First get the data for all ou runners
        for filename in sorted(glob.glob(os.path.join(self.input_path, '*.csv'))):
            print(filename)
            data_list = opunit_data.get_ou_runner_data(filename, self.model_metrics_path, self.txn_sample_rate,
                                                         self.model_map, self.stats_map, self.trim)
            for data in data_list:
                best_y_transformer, best_method = self.train_data(data, summary_file)
                if self.expose_all:
                    self.train_specific_model(data, best_y_transformer, best_method)

        return self.model_map


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='OU Model Trainer')
    aparser.add_argument('--input_path', default='modeling/ou_runner_input',
                         help='Input file path for the ou runners')
    aparser.add_argument('--model_results_path', default='modeling/ou_runner_model_results',
                         help='Prediction results of the ou models')
    aparser.add_argument('--save_path', default='modeling/trained_model', help='Path to save the ou models')
    aparser.add_argument('--ml_models', nargs='*', type=str,
                         default=["lr", "rf", "gbm"],
                         help='ML models for the ou trainer to evaluate')
    aparser.add_argument('--test_ratio', type=float, default=0.2, help='Test data split ratio')
    aparser.add_argument('--trim', default=0.2, type=float, help='% of values to remove from both top and bottom')
    aparser.add_argument('--expose_all', default=True, help='Should expose all data to the model')
    aparser.add_argument('--txn_sample_rate', type=int, default=2,
                         help='Sampling rate percentage for the transaction OUs (ignored if 0)')
    aparser.add_argument('--log', default='info', help='The logging level')
    args = aparser.parse_args()

    logging_util.init_logging(args.log)
    trainer = OUModelTrainer(args.input_path, args.model_results_path, args.ml_models, args.test_ratio, args.trim,
                             args.expose_all, args.txn_sample_rate)
    trained_model_map = trainer.train()
    with open(args.save_path + '/ou_model_map.pickle', 'wb') as file:
        pickle.dump((trained_model_map, data_info.instance), file)
