#!/usr/bin/env python3

import unittest
import subprocess
import pickle
import logging

from pathlib import Path
from data_class import opunit_data
from mini_trainer import MiniTrainer
from sklearn import model_selection
from util import io_util, logging_util


# Mini trainer input diretory
TEST_DATA_DIR = Path('test_data').resolve()

# Test timeout period
TEST_TIMEOUT = 120

# Mini runner built binary path
MINI_RUNNER_BINARY_PATH = Path('../../cmake-build-release/release/mini_runners').resolve()

# Where the dummy model map is tored
TEST_MODEL_MAP_PATH = TEST_DATA_DIR / 'mini_model_map.pickle'


logging_util.init_logging('warning')

class ModelServerTest(unittest.TestCase):

    def test_inference(self):
        """
        Load the test model maps, and test if inference could be performed with the model
        """

        # Test parameters
        data_file = TEST_DATA_DIR / 'seq_files' / 'execution_SEQ0_gen.csv'
        result_path = ""
        test_ratio = 0.9
        trim = 0.2
        model_map_path = str(TEST_MODEL_MAP_PATH)

        # Prepare the test data
        data = opunit_data.get_mini_runner_data(str(data_file), result_path, {}, {}, trim)[0]
        x_train, x_test, y_train, y_test = model_selection.train_test_split(data.x, data.y,
                                                                            test_size=test_ratio,
                                                                            random_state=0)
        # Load the model map
        with open(model_map_path, 'rb') as f:
            model_map = pickle.load(f)

        for opunit, model in model_map.items():
            y_pred = model.predict(x_test)
            self.assertNotEqual(len(y_pred), 0)

        logging.warning("Test Inference OK")

    def test_generate(self):
        """
        Invoke the mini_runners to generate csv files
        """
        benchmark_filters = '--benchmark_filters=SEQ0'
        args = [benchmark_filters]
        try:
            proc = subprocess.run(args=[str(MINI_RUNNER_BINARY_PATH)] + args, check=True, encoding='utf-8',
                                  capture_output=True, timeout=TEST_TIMEOUT)
        except subprocess.CalledProcessError as e:
            self.fail(f'Running mini runner fails with errors: {e.stderr}')

        logging.warning("Test Generate OK")

    def test_train(self):
        """
        Given trace files, the mini_trainer should be able to produce
        training maps.
        """
        train_dir = TEST_DATA_DIR / 'seq_files'
        result_path = "mini_runner_model_results"
        ml_models = ["lr", "rf", "gbm"]
        test_ratio = 0.2
        trim = 0.2
        expose_all = True

        trainer = MiniTrainer(str(train_dir), result_path, ml_models, test_ratio, trim, expose_all)
        model_map = trainer.train()

        # The output map should not be empty
        self.assertNotEqual(model_map, {})

        logging.warning("Test Training OK")

if __name__ == '__main__':
    unittest.main()
