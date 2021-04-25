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

import atexit
import datetime
import enum
import json
import logging
import os
import pickle
import pprint
import sys
from abc import ABC, abstractmethod
from enum import Enum, IntEnum, auto
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import zmq
from forecasting.forecaster import Forecaster, parse_model_config
from modeling.info import data_info
from modeling.interference_model_trainer import InterferenceModelTrainer
from modeling.ou_model_trainer import OUModelTrainer
from modeling.type import OpUnit
from modeling.util import logging_util

logging_util.init_logging('info')


class ModelType(enum.IntEnum):
    """ModelType
    """
    FORECAST = 0,
    OPERATING_UNIT = 1
    INTERFERENCE = 2


class MessengerCallback(IntEnum):
    """
    Messenger callback IDs.
    Keep this in sync with Messenger::BuiltinCallback, especially ACK.
    """
    NOOP = 0
    ECHO = 1
    ACK = 2


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
    TRAIN = auto()  # Train a specific model
    QUIT = auto()  # Quit the server
    PRINT = auto()  # Print the message
    INFER = auto()  # Do inference on a trained model

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
    The message format has to be kept consistent with the C++ Messenger.
    A valid message is :
        "msg_id-send_id-recv_id-payload"

    Refer to Messenger's documentation for the message format.
    """

    def __init__(self, cmd: Optional[Command] = None,
                 data: Optional[Dict] = None) -> None:
        self.cmd = cmd
        self.data = data

    @staticmethod
    def from_json(json_str: str) -> Optional[Message]:
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


class AbstractModel(ABC):
    """
    Interface for all the models
    """

    def __init__(self) -> None:
        # Model cache that maps from the model path on disk to the model
        self.model_cache = dict()

    @abstractmethod
    def train(self, data: Dict) -> Tuple[bool, str]:
        """
        Perform fitting.
        Should be overloaded by a specific model implementation.
        :param data: data used for training
        :return: if training succeeds, {True and empty string}, else {False, error message}
        """
        raise NotImplementedError("Should be implemented by child classes")

    @abstractmethod
    def infer(self, data: Dict) -> Tuple[Any, bool, str]:
        """
        Do inference on the model, give the data file, and the model_map_path
        :param data: data used for inference
        :return: {List of predictions, if inference succeeds, error message}
        """
        raise NotImplementedError("Should be implemented by child classes")

    def _load_model(self, save_path: str):
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

        # use the path string as the key of the cache
        save_path_str = str(save_path)

        # Load from cache
        if self.model_cache.get(save_path_str, None) is not None:
            return self.model_cache[save_path_str]

        # Load into cache
        model = self._load_model_from_disk(save_path)

        self.model_cache[save_path_str] = model
        return model

    @abstractmethod
    def _load_model_from_disk(self, save_path: Path):
        """
        Load model from the path on disk (invoked when missing model cache)
        :param save_path: model path on disk
        :return: model for the child class' specific model type
        """
        raise NotImplementedError("Should be implemented by child classes")


class OUModel(AbstractModel):
    """
    OUModel that handles training and inference for OU models
    """

    # Training parameters
    TEST_RATIO = 0.2
    TRIM_RATIO = 0.2
    EXPOSE_ALL = True
    TXN_SAMPLE_RATE = 2

    def __init__(self) -> None:
        AbstractModel.__init__(self)

    def train(self, data: Dict) -> Tuple[bool, str]:
        """
        Train a model with the given model name and seq_files directory
        :param data: {
            methods: [lr, XXX, ...],
            input_path: PATH_TO_SEQ_FILES_FOLDER, or None
            save_path: PATH_TO_SAVE_MODEL_MAP
        }
        :return: if training succeeds, {True and empty string}, else {False, error message}
        """
        ml_models = data["methods"]
        seq_files_dir = data["input_path"]
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

        test_ratio = OUModel.TEST_RATIO
        trim = OUModel.TRIM_RATIO
        expose_all = OUModel.EXPOSE_ALL
        txn_sample_rate = OUModel.TXN_SAMPLE_RATE

        trainer = OUModelTrainer(seq_files_dir, result_path, ml_models,
                                 test_ratio, trim, expose_all, txn_sample_rate)
        # Perform training from OUModelTrainer and input files directory
        model_map = trainer.train()

        # Pickle dump the model
        with save_path.open(mode='wb') as f:
            pickle.dump((model_map, data_info.instance), f)

        return True, ""

    def infer(self, data: Dict) -> Tuple[Any, bool, str]:
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

        # Load the model map
        model_map = self._load_model(model_path)
        if model_map is None:
            logging.error(
                f"Model map at {str(model_path)} has not been trained")
            return [], False, "MODEL_MAP_NOT_TRAINED"

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

        model = model_map[opunit]
        if model is None:
            logging.error(f"Model for {opunit} doesn't exist")
            return [], False, "MODEL_NOT_FOUND"

        y_pred = model.predict(features)
        return y_pred.tolist(), True, ""

    def _load_model_from_disk(self, save_path: Path) -> Dict:
        """
        Load model from the path on disk (invoked when missing model cache)
        :param save_path: model path on disk
        :return: OU model map
        """
        with save_path.open(mode='rb') as f:
            model, data_info.instance = pickle.load(f)
        return model


class InterferenceModel(AbstractModel):
    """
    InterferenceModel that handles training and inference for the interference model
    """

    # Training parameters
    TEST_RATIO = 0.2
    IMPACT_MODEL_RATIO = 0.1
    WARMUP_PERIOD = 3
    USE_QUERY_PREDICT_CACHE = False
    ADD_NOISE = False
    PREDICT_OU_ONLY = False
    TXN_SAMPLE_RATE = 2
    NETWORK_SAMPLE_RATE = 2

    def __init__(self) -> None:
        AbstractModel.__init__(self)

    def train(self, data: Dict) -> Tuple[bool, str]:
        """
        Train a model with the given model name and seq_files directory
        :param data: {
            methods: [lr, XXX, ...],
            input_path: PATH_TO_SEQ_FILES_FOLDER, or None
            save_path: PATH_TO_SAVE_MODEL_MAP
        }
        :return: if training succeeds, {True and empty string}, else {False, error message}
        """
        ml_models = data["methods"]
        input_path = data["input_path"]
        save_path = data["save_path"]
        ou_model_path = data["ou_model_path"]
        ee_sample_rate = data["pipeline_metrics_sample_rate"]

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

        test_ratio = InterferenceModel.TEST_RATIO
        impact_model_ratio = InterferenceModel.IMPACT_MODEL_RATIO
        warmup_period = InterferenceModel.WARMUP_PERIOD
        use_query_predict_cache = InterferenceModel.USE_QUERY_PREDICT_CACHE
        add_noise = InterferenceModel.ADD_NOISE
        predict_ou_only = InterferenceModel.PREDICT_OU_ONLY
        txn_sample_rate = InterferenceModel.TXN_SAMPLE_RATE
        network_sample_rate = InterferenceModel.NETWORK_SAMPLE_RATE

        with open(ou_model_path, 'rb') as pickle_file:
            model_map, data_info.instance = pickle.load(pickle_file)
        trainer = InterferenceModelTrainer(input_path, result_path, ml_models, test_ratio, impact_model_ratio,
                                           model_map, warmup_period, use_query_predict_cache, add_noise,
                                           predict_ou_only, ee_sample_rate, txn_sample_rate, network_sample_rate)

        # Perform training
        trainer.predict_ou_data()
        # We only need the directly model for the model server. The other models are for experimental purposes
        _, _, direct_model = trainer.train()

        # Pickle dump the model
        with open(save_path, 'wb') as file:
            pickle.dump(direct_model, file)

        return True, ""

    def infer(self, data: Dict) -> Tuple[Any, bool, str]:
        """
        Do inference on the model, give the data file, and the model_path
        :param data: {
            features: 2D float arrays [[float]],
            model_path: model path
        }
        :return: {List of predictions, if inference succeeds, error message}
        """
        features = data["features"]
        model_path = data["model_path"]

        # Load the model
        model = self._load_model(model_path)
        if model is None:
            logging.error(
                f"Model map at {str(model_path)} has not been trained")
            return [], False, "MODEL_MAP_NOT_TRAINED"

        features = np.array(features)

        y_pred = model.predict(features)
        return y_pred.tolist(), True, ""

    def _load_model_from_disk(self, save_path: Path):
        """
        Load model from the path on disk (invoked when missing model cache)
        :param save_path: model path on disk
        :return: interference model
        """
        with save_path.open(mode='rb') as f:
            model = pickle.load(f)
        return model


class ForecastModel(AbstractModel):
    """
    ForecastModel that handles training and inference for Forecast models
    """

    # Number of Microseconds per second
    MICRO_SEC_PER_SEC = 1000000

    def __init__(self) -> None:
        AbstractModel.__init__(self)

    def train(self, data: Dict) -> Tuple[bool, str]:
        """
        Train a model with the given model name and seq_files directory
        :param data: {
            model_names: [LSTM...]
            models_config: PATH_TO_JSON model config file
            input_path: PATH_TO_TRACE, or None
            input_sequence: Trace sequence or None
            save_path: PATH_TO_SAVE_MODEL_MAP
            interval_micro_sec: Interval duration for aggregation in microseconds
            sequence_length: Number of intervals in a data sequence
            horizon_length: Number of intervals ahead for the planning horizon
        }
        :return: if training succeeds, {True and empty string}, else {False, error message}
        """
        input_path = data["input_path"] if "input_path" in data else None
        input_sequence = data["input_sequence"] if "input_sequence" in data else None
        save_path = data["save_path"]
        model_names = data["methods"]
        models_config = data.get("models_config")
        interval = data["interval_micro_sec"]
        seq_length = data["sequence_length"]
        horizon_length = data["horizon_length"]

        # Parse models arguments
        models_kwargs = parse_model_config(model_names, models_config)

        # Do path checking up-front
        save_path = Path(save_path)
        save_dir = save_path.parent
        try:
            # Exist ok, and Creates parent if ok
            save_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError as e:
            return False, "FAIL_PERMISSION_ERROR"

        if input_path is None and input_sequence is None:
            return False, "NO_INPUT_PROVIDED"

        if input_path is not None and input_sequence is not None:
            return [], False, "BOTH_INPUT_PATH_AND_SEQUENCE_SPECIFIED"

        forecaster = Forecaster(
            trace_file=input_path,
            trace_sequence=input_sequence,
            interval_us=interval,
            test_mode=False,
            seq_len=seq_length,
            eval_size=seq_length + 2 * horizon_length,
            horizon_len=horizon_length)

        models = forecaster.train(models_kwargs)

        # Pickle dump the model
        with save_path.open(mode='wb') as f:
            pickle.dump(models, f)

        return True, ""

    def infer(self, data: Dict) -> Tuple[Any, bool, str]:
        """
        Do inference on the model, give the data file, and the model_map_path
        :param data: {
            input_path: PATH_TO_TRACE, or None
            input_sequence: Input sequence data, or None
            model_path: model path
            model_names: [LSTM...]
            models_config: PATH_TO_JSON model config file
            interval_micro_sec: Interval duration for aggregation in microseconds
            sequence_length: Number of intervals in a data sequence
            horizon_length: Number of intervals ahead for the planning horizon
        }
        :return: {Dict<cluster, Dict<query>, List<preds>>, if inference succeeds, error message}
        """
        input_path = data["input_path"] if "input_path" in data else None
        input_sequence = data["input_sequence"] if "input_sequence" in data else None
        model_names = data["model_names"]
        models_config = data.get("models_config")
        interval = data["interval_micro_sec"]
        seq_length = data["sequence_length"]
        horizon_length = data["horizon_length"]
        model_path = data["model_path"]

        # Load the trained models
        models = self._load_model(model_path)
        if models is None:
            logging.error(
                f"Models at {str(model_path)} has not been trained")
            return [], False, "MODELS_NOT_TRAINED"

        if input_path is None and input_sequence is None:
            return [], False, "NO_INPUT_PROVIDED"

        if input_path is not None and input_sequence is not None:
            return [], False, "BOTH_INPUT_PATH_AND_SEQUENCE_SPECIFIED"

        forecaster = Forecaster(
            trace_file=input_path,
            trace_sequence=input_sequence,
            test_mode=True,
            interval_us=interval,
            seq_len=seq_length,
            eval_size=seq_length + 2 * horizon_length,
            horizon_len=horizon_length)

        # FIXME:
        # Assuming all the queries in the current trace file are from
        # the same cluster for now

        # Only forecast with first element of model_names
        result = {}
        query_pred = forecaster.predict(0, models[0][model_names[0]])
        for qid, ts in query_pred.items():
            result[int(qid)] = ts
        return {0: result}, True, ""

    def _load_model_from_disk(self, save_path: Path):
        """
        Load model from the path on disk (invoked when missing model cache)
        :param save_path: model path on disk
        :return: workload forecasting model
        """
        with save_path.open(mode='rb') as f:
            model = pickle.load(f)
        return model


class ModelServer:
    """
    ModelServer(MS) class that runs in a loop to handle commands from the ModelServerManager from C++
    """

    POLL_TIMEOUT = 250  # milliseconds
    RETRY_TIMEOUT = 10  # seconds

    def __init__(self, end_point: str):
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

        # Global model map cache
        self.cache = dict()

        # Used to automatically resend messages until the messages are ack'd.
        self.next_msg_id = 0
        self.pending_msgs = {}

        # Notify the ModelServerManager that I am connected
        self._send_msg(self._next_msg_id(), 0, 0, ModelServer._make_response(
            Callback.CONNECTED, "", True, ""))

        # Model trainers/inferers
        self.model_managers = {ModelType.FORECAST: ForecastModel(),
                               ModelType.OPERATING_UNIT: OUModel(),
                               ModelType.INTERFERENCE: InterferenceModel()}

    def _next_msg_id(self):
        msg_id = self.next_msg_id
        self.next_msg_id += 1
        return msg_id

    def cleanup_zmq(self):
        """
        Close the socket when the script exits
        :return:
        """
        self.socket.close()
        self.context.destroy()

    def _send_msg(self, msg_id: int, send_id: int, recv_id: int, data: Dict) -> None:
        """
        Send a message to the socket.
        :param msg_id: id of this message
        :param send_id: callback id on this end, 0 for now
        :param recv_id: callback id to invoke on the other end
        :param data: payload of the message in JSON
        :return:
        """
        json_result = json.dumps(data)
        msg = f"{msg_id}-{send_id}-{recv_id}-{json_result}"
        if msg_id not in self.pending_msgs:
            self.pending_msgs[msg_id] = (send_id, recv_id, data), datetime.datetime.now()
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
    def _parse_msg(payload: str) -> Tuple[int, int, int, Optional[Message]]:
        logging.debug("PY RECV: " + payload)
        tokens = payload.split('-', 3)

        # Invalid message format
        try:
            msg_id = int(tokens[0])
            send_cb_id = int(tokens[1])
            recv_cb_id = int(tokens[2])
        except ValueError as e:
            logging.error(
                f"Invalid message payload format: {payload}, ids not int.")
            return -1, -1, -1, None

        msg = Message() if tokens[3] == "" else Message.from_json(tokens[3])
        return msg_id, send_cb_id, recv_cb_id, msg

    def _infer(self, data: Dict) -> Tuple[List, bool, str]:
        """
        Do inference on the model
        :param data: {
            type: model type
            model_path: model path
            ...
        }
        :return: {List of predictions, if inference succeeds, error message}
        """
        model_type = data["type"]
        return self.model_managers[ModelType[model_type]].infer(data)

    def _recv(self) -> str:
        """
        Receive from the ZMQ socket. This is a blocking call.

        :return: Message payload
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
                model_type = data["type"]
                ok, res = self.model_managers[ModelType[model_type]].train(data)
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

        while (1):
            try:
                data_available = False
                while not data_available:
                    now = datetime.datetime.now()
                    # Try sending any pending messages that haven't been acknowledged yet.
                    for msg_id in self.pending_msgs:
                        (send_id, recv_id, data), last_sent = self.pending_msgs[msg_id]
                        if (last_sent - now).seconds > ModelServer.RETRY_TIMEOUT:
                            self._send_msg(msg_id, send_id, recv_id, data)
                    # Poll for up to 250 milliseconds to see if there are any incoming events.
                    data_available = 0 != self.socket.poll(ModelServer.POLL_TIMEOUT)
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

            msg_id, send_id, recv_id, msg = self._parse_msg(payload)

            # If this is an acknowledgment, clear the corresponding pending message.
            if recv_id == MessengerCallback.ACK:
                self.pending_msgs.pop(msg_id, None)
                continue
            if msg is None:
                continue
            else:
                result, cont = self._execute_cmd(msg.cmd, msg.data)
                if not cont:
                    logging.info("Shutting down.")
                    break

                # Currently not expecting to invoke any callback on ModelServer
                # side, so second parameter 0
                self._send_msg(self._next_msg_id(), 0, send_id, result)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ./model_server.py <ZMQ_ENDPOINT>")
        exit(-1)
    ms = ModelServer(sys.argv[1])
    ms.run_loop()
