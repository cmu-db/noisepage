#!/usr/bin/env python3

import os
import unittest
import subprocess
import pickle
import logging

from shutil import copy2, move, rmtree
from pathlib import Path
from data_class import opunit_data
from mini_trainer import MiniTrainer
from sklearn import model_selection
from util import io_util, logging_util

# Project Root
# Assume this file is located in ROOT/model/script
NOISEPAGE_PROJECT_ROOT = Path('../..').resolve()

RELEASE_BUILD_SUBPATH = 'cmake-build-release'
DEBUG_BUILD_SUBPATH = 'cmake-build-debug'

# Mini trainer input diretory
TEST_DATA_DIR = Path('/tmp/test_data').resolve()
TEST_SEQ_FILES_DIR = TEST_DATA_DIR/'seq_files'

# Test timeout period
TEST_TIMEOUT = 120

# Mini runner built binary path
MINI_RUNNER_BINARY_PATH = NOISEPAGE_PROJECT_ROOT / DEBUG_BUILD_SUBPATH / 'benchmark/mini_runners'

# Where the dummy model map is tored
TEST_MODEL_MAP_PATH = TEST_DATA_DIR / 'mini_model_map.pickle'

# bytecode_handlers_ir.bc required to run the mini_runners
BYTECODE_HANDLER_IR_PATH = NOISEPAGE_PROJECT_ROOT / DEBUG_BUILD_SUBPATH / 'bin/bytecode_handlers_ir.bc'

# Mini trainer results dir
MINI_TRAINER_RESULT_DIR = 'test_mini_runner_model_results'

# Inference dummy input file
TEST_INFERENCE_FILE_PATH= TEST_DATA_DIR / 'seq_files' / 'SEQ0_execution.csv'

logging_util.init_logging('warning')

class ModelServerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Checks for necessary dependencies of the testsuite.
        :return:
        """
        # 1. Check mini_runner
        if not MINI_RUNNER_BINARY_PATH.exists():
            raise FileNotFoundError(f"{str(MINI_RUNNER_BINARY_PATH)} not found. Please build mini_runner target!")

        # 2. Check the bytecode_handlers_ir.bc exists
        if not BYTECODE_HANDLER_IR_PATH.exists():
            raise FileNotFoundError(f"{str(BYTECODE_HANDLER_IR_PATH)} not found. "
                                    f"Please build hack_bytecode_handlers_ir target!")

        # Copy that to the local directory (This hardcoded in llvm_engne.h:GetBytecodeHandlersBcPath)
        copy2(BYTECODE_HANDLER_IR_PATH, os.getcwd())

        # 3. Check test data is there
        TEST_DATA_DIR.mkdir(exist_ok=True, parents=True)
        TEST_SEQ_FILES_DIR.mkdir(exist_ok=True, parents=True)

        # 4. Check the result dir exists
        if not os.path.exists(MINI_TRAINER_RESULT_DIR):
            os.mkdir(MINI_TRAINER_RESULT_DIR)


    @classmethod
    def tearDownClass(cls):
        # Remove the bytecode_handlers_ir.bc
        os.remove('bytecode_handlers_ir.bc')

        # Remove all data generated
        rmtree(MINI_TRAINER_RESULT_DIR)
        rmtree(TEST_DATA_DIR)

    def generate_seq_file(self):
        """
        Generate the trace files in TEST_SEQ_FILES_DIR

        :return:  file path generated
        """
        benchmark_filters = '--benchmark_filter='+ 'SEQ0'
        mini_runner_rows_limit = '--mini_runner_rows_limit=' + '10000'

        args = [benchmark_filters, mini_runner_rows_limit]
        try:
            proc = subprocess.run(args=[str(MINI_RUNNER_BINARY_PATH)] + args, check=True, encoding='utf-8',
                                  capture_output=True, timeout=TEST_TIMEOUT)
        except subprocess.CalledProcessError as e:
            self.fail(f'Running mini runner fails with errors: {e.stderr}')

        # Move the seqfiles to test
        return move('pipeline.csv', str(TEST_SEQ_FILES_DIR / 'SEQ0_execution.csv'))

    def train_model(self, train_dir, result_path):
        """
        Train a mini_model
        :param train_dir:  Where seq files are stored
        :param result_path:  Result path for the mini_trainer
        :return:
        """
        ml_models = ["lr", "rf", "gbm"]
        test_ratio = 0.2
        trim = 0.2
        expose_all = True

        trainer = MiniTrainer(train_dir, result_path, ml_models, test_ratio, trim, expose_all)
        model_map = trainer.train()
        return model_map


    def test_inference(self):
        """
        Load the test model maps, and test if inference could be performed with the model
        """

        # Test parameters
        file_path = self.generate_seq_file()
        result_path = MINI_TRAINER_RESULT_DIR
        model_map = self.train_model(str(TEST_SEQ_FILES_DIR), result_path)

        # Prepare the test data
        result_path = ""
        test_ratio = 0.9
        trim = 0.2
        data = opunit_data.get_mini_runner_data(str(file_path), result_path, {}, {}, trim)[0]
        x_train, x_test, y_train, y_test = model_selection.train_test_split(data.x, data.y,
                                                                            test_size=test_ratio,
                                                                            random_state=0)

        for opunit, model in model_map.items():
            y_pred = model.predict(x_test)
            self.assertNotEqual(len(y_pred), 0)

        logging.warning("Test Inference OK")


    def test_train(self):
        """
        Given trace files, the mini_trainer should be able to produce
        training maps.
        """
        # Generate test sequence
        self.generate_seq_file()

        train_dir = TEST_SEQ_FILES_DIR
        result_path = MINI_TRAINER_RESULT_DIR

        model_map = self.train_model(str(train_dir),result_path)

        # The output map should not be empty
        self.assertNotEqual(model_map, {})

        logging.warning("Test Training OK")

if __name__ == '__main__':
    unittest.main()
