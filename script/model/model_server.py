#!/usr/bin/env python3
"""
This file contains the python ModelServer implementation.

Invoke with:
    `model_server.py <ZMQ_ENDPOINT>`

The server should be stateless but with caching of models.
The message format that the ModelServer expects should be kept consistent with Messenger class in
the noisepage source code.
The command format should be kept with ModelServerManager in the noisepage source
code.

TODO(Ricky):
- Design an Error code scheme for ModelServerManager and ModelServer.
    Like some string errors? This should be transparent to the user of the ModelServerManager so I
    am delaying this to next PR.

"""

from __future__ import annotations
import sys
import atexit
from enum import Enum, auto, IntEnum
from typing import Dict, Optional, Tuple, List, Any
import json
import logging
import os
import pprint
import pickle
from pathlib import Path

import numpy as np
import zmq

from data_class import opunit_data
from mini_trainer import MiniTrainer
from util import logging_util
from type import OpUnit
from info import data_info

logging_util.init_logging('info')


class Callback(IntEnum):
    """
    ModelServerManager <==> ModelServer callback Id.
    Needs to be kept consistent with ModelServerManager.h's Callback Enum
    """
    NOOP = 0
    CONNECTED = 1


class Command(Enum):
    """
    Command enum for actions to take from the manager.
    This has to be kept consistent with the C++ ModelServerManager.
    """
    TRAIN = auto()      # Train a specific model
    QUIT = auto()       # Quit the server
    PRINT = auto()      # Print the message
    INFER = auto()      # Do inference on a trained model

    def __str__(self) -> str:
        return self.name

    @staticmethod
    def from_str(cmd_str: str) -> Command:
        if cmd_str == "PRINT":
            return Command.PRINT
        elif cmd_str == "QUIT":
            return Command.QUIT
        elif cmd_str == "TRAIN":
            return Command.TRAIN
        elif cmd_str == "INFER":
            return Command.INFER
        else:
            raise ValueError("Invalid command")


class Message:
    """
    Message struct for communication with the ModelServer.
    The message format has to be ketp consistent with the C++ Messenger.
    A valid message is :
        "send_id-recv_id-payload"

    Refer to Messenger's documention for the message format
    """

    def __init__(self, cmd: Optional[Command] = None,
                 data: Optional[Dict] = None) -> None:
        self.cmd = cmd
        self.data = data

    @staticmethod
    def from_json(json_str: str) -> Message:
        d = json.loads(json_str)
        msg = Message()
        try:
            msg.cmd = Command.from_str(d["cmd"])
            msg.data = d["data"]
        except (KeyError, ValueError) as e:
            logging.error(f"Invalid Message : {json_str}")
            return None

        return msg

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

    def __str__(self) -> str:
        return pprint.pformat(self.__dict__)


class ModelServer:
    """
    ModelServer(MS) class that runs in a loop to handle commands from the ModelServerManager from C++
    """

    # Training parameters
    TEST_RATIO = 0.2
    TRIM_RATIO = 0.2
    EXPOSE_ALL = True
    TXN_SAMPLE_INTERVAL = 49

    def __init__(self, end_point: str) -> ModelServer:
        """
        Initialize the ModelServer by connecting to the ZMQ IPC endpoint
        :param end_point:  IPC endpoint
        """
        # Establish ZMQ connection
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.set_string(zmq.IDENTITY, 'model')
        logging.debug(
            f"Python model trying to connect to manager at {end_point}")
        self.socket.connect(f"ipc://{end_point}")
        logging.info(f"Python model connected at {end_point}")

        # If the ModelServer is closing
        self._closing = False

        # Register the exit callback
        atexit.register(self.cleanup_zmq)

        # Gobal model map cache
        self.cache = dict()

        # Notify the ModelServerManager that I am connected
        self._send_msg(0, 0, ModelServer._make_response(
            Callback.CONNECTED, "", True, ""))

    def cleanup_zmq(self):
        """
        Close the socket when the script exits
        :return:
        """
        self.socket.close()
        self.context.destroy()

    def _send_msg(self, send_id: int, recv_id: int, data: Dict) -> None:
        """
        Send a message to the socket.
        :param send_id: id on this end, 0 for now
        :param recv_id: callback id to invoke on the other end
        :param data: payload of the message in JSON
        :return:
        """
        json_result = json.dumps(data)
        msg = f"{send_id}-{recv_id}-{json_result}"
        self.socket.send_multipart([''.encode('utf-8'), msg.encode('utf-8')])

    @staticmethod
    def _make_response(action: Callback, result: Any, success: bool, err: str = "") -> Dict:
        """
        Construct a response to the ModelServerManager
        :param action:  Action callback on the ModelServerManager
        :param result: Any result
        :param success: True if the action suceeds
        :param err: Error message
        :return:
        """
        return {
            "action": action,
            "result": result,
            "success": success,
            "err": err
        }

    @staticmethod
    def _parse_msg(payload: str) -> Tuple[int, int, Message]:
        logging.debug("PY RECV: " + payload)
        tokens = payload.split('-', 2)

        # Invalid message format
        try:
            msg_id = int(tokens[0])
            recv_id = int(tokens[1])
        except ValueError as e:
            logging.error(
                f"Invalid message payload format: {payload}, ids not int.")
            return -1, -1, None

        msg = Message.from_json(tokens[2])
        return msg_id, recv_id, msg

    @staticmethod
    def _transform_opunit_data(raw_data: Dict) -> List[opunit_data.OpUnitData]:
        """
        Transform the raw training data from ModelServerManger for each data unit
        :param raw_data: {
            opunit: (x, y)
        }
        :return:
        """
        # TODO(ricky): serialize the opunit by name rather than index
        return [opunit_data.OpUnitData(
            OpUnit(x[0]), x[1][0], x[1][1]) for x in raw_data]

    def _train_model(self, data: Dict) -> Tuple[bool, str]:
        """
        Train a model with the given model name and seq_files directory
        :param data: {
            methods: [lr, XXX, ...],
            seq_files: PATH_TO_SEQ_FILES_FOLDER, or None
            save_path: PATH_TO_SAVE_MODEL_MAP
        }
        :return: if training succeeds, {True and empty string}, else {False, error message}
        """
        ml_models = data["methods"]
        seq_files_dir = data["seq_files"]
        save_path = data["save_path"]

        # Do path checking up-front
        save_path = Path(save_path)
        save_dir = save_path.parent
        try:
            # Exist ok, and Creates parent if ok
            save_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError as e:
            return False, "FAIL_PERMISSION_ERROR"

        # Create result model metrics in the same directory
        save_file_name = save_path.stem
        result_path = save_path.with_name(
            str(save_file_name) + "_metric_results")
        result_path.mkdir(parents=True, exist_ok=True)

        test_ratio = ModelServer.TEST_RATIO
        trim = ModelServer.TRIM_RATIO
        expose_all = ModelServer.EXPOSE_ALL
        txn_sample_interval = ModelServer.TXN_SAMPLE_INTERVAL

        trainer = MiniTrainer(seq_files_dir, result_path, ml_models,
                              test_ratio, trim, expose_all, txn_sample_interval)
        # Perform training from MiniTrainer and input files directory
        model_map = trainer.train()
        self.cache[str(save_path)] = model_map

        # Pickle dump the model
        with save_path.open(mode='wb') as f:
            pickle.dump((model_map, data_info.instance), f)

        return True, ""

    def _load_model_map(self, save_path: str) -> Optional[Dict]:
        """
        Check if a trained model exists at the path.
        Load the model into cache if it is not.
        :param save_path: path to model to load
        :return: None if no model exists at path, or Model map saved at path
        """
        save_path = Path(save_path)

        # Check model exists
        if not save_path.exists():
            return None

        # Load from cache
        if self.cache.get(save_path, None) is not None:
            return self.cache[save_path]

        # Load into cache
        with save_path.open(mode='rb') as f:
            model, data_info.instance = pickle.load(f)

            # TODO(ricky): model checking here?
            if len(model) == 0:
                logging.warning(f"Empty model at {str(save_path)}")
                return None

            self.cache[str(save_path)] = model
            return model

    def _infer(self, data: Dict) -> Tuple[List, bool, str]:
        """
        Do inference on the model, give the data file, and the model_map_path
        :param data: {
            features: 2D float arrays [[float]],
            opunit: Opunit integer for the model
            model_path: model path
        }
        :return: {List of predictions, if inference succeeds, error message}
        """
        features = data["features"]
        opunit = data["opunit"]
        model_path = data["model_path"]

        # Parameter validation
        if not isinstance(opunit, str):
            return [], False, "INVALID_OPUNIT"
        try:
            opunit = OpUnit[opunit]
        except KeyError as e:
            logging.error(f"{opunit} is not a valid Opunit name")
            return [], False, "INVALID_OPUNIT"

        features = np.array(features)
        logging.debug(f"Using model on {opunit}")

        # Load the model map
        model_map = self._load_model_map(model_path)
        if model_map is None:
            logging.error(
                f"Model map at {str(model_path)} has not been trained")
            return [], False, "MODEL_MAP_NOT_TRAINED"

        model = model_map[opunit]

        if model is None:
            logging.error(f"Model for {opunit} doesn't exist")
            return [], False, "MODEL_NOT_FOUND"

        y_pred = model.predict(features)

        return y_pred.tolist(), True, ""

    def _recv(self) -> str:
        """
        Receive from the ZMQ socket. This is a blocking call.

        :return: Message paylod
        """
        identity = self.socket.recv()
        _delim = self.socket.recv()
        payload = self.socket.recv()
        logging.debug(f"Python recv: {str(identity)}, {str(payload)}")

        return payload.decode("ascii")

    def _execute_cmd(self, cmd: Command, data: Dict) -> Tuple[Dict, bool]:
        """
        Execute a command from the ModelServerManager
        :param cmd:
        :param data:
        :return: Tuple {
            message string to sent back,
            if continue the server
        }
        """
        if cmd == Command.PRINT:
            msg = data["message"]
            logging.info(f"MESSAGE PRINT: {str(msg)}")
            response = self._make_response(Callback.NOOP, f"MODEL_REPLY_{msg}", True)
            return response, True
        elif cmd == Command.QUIT:
            # Will not send any message so empty {} is ok
            return self._make_response(Callback.NOOP, "", True), False
        elif cmd == Command.TRAIN:
            try:
                ok, res = self._train_model(data)
                if ok:
                    response = self._make_response(Callback.NOOP, res, True)
                else:
                    response = self._make_response(Callback.NOOP, "", False, res)
            except ValueError as e:
                logging.error(f"Model Not found : {e}")
                response = self._make_response(
                    Callback.NOOP, "", False, "FAIL_MODEL_NOT_FOUND")
            except KeyError as e:
                logging.error(f"Data format wrong for TRAIN: {e}")
                response = self._make_response(
                    Callback.NOOP, "", False, "FAIL_DATA_FORMAT_ERROR")
            except Exception as e:
                logging.error(f"Training failed. {e}")
                response = self._make_response(
                    Callback.NOOP, "", False, "FAIL_TRAINING_FAILED")

            return response, True
        elif cmd == Command.INFER:
            result, ok, err = self._infer(data)
            response = self._make_response(Callback.NOOP, result, ok, err)
            return response, True

    def run_loop(self):
        """
        Run in a loop to recv/send message to the ModelServer manager
        :return:
        """

        while(1):
            try:
                payload = self._recv()
            except UnicodeError as e:
                logging.warning(f"Failed to decode : {e.reason}")
                continue
            except KeyboardInterrupt:
                if self._closing:
                    logging.warning("Forced shutting down now.")
                    os._exit(-1)
                else:
                    logging.info("Received KeyboardInterrupt. Ctrl+C again to force shutting down.")
                    self._closing = True
                    continue

            send_id, recv_id, msg = self._parse_msg(payload)
            if msg is None:
                continue
            else:
                result, cont = self._execute_cmd(msg.cmd, msg.data)
                if not cont:
                    logging.info("Shutting down.")
                    break

                # Currently not expecting to invoke any callback on ModelServer
                # side, so second parameter 0
                self._send_msg(0, send_id, result)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ./model_server.py <ZMQ_ENDPOINT>")
        exit(-1)
    ms = ModelServer(sys.argv[1])
    ms.run_loop()
