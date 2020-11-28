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
- Encapsulate the ModelServer in a class

"""

from __future__ import annotations
import sys
import atexit
from enum import Enum, auto, IntEnum
from typing import Dict, Optional, Tuple, List, Any
import json
import logging
import pprint
import pickle
import numpy as np
import zmq

from data_class import opunit_data
from mini_trainer import MiniTrainer
from sklearn import model_selection
from util import logging_util
from type import OpUnit


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
    def __init__(self, cmd: Optional[Command] = None, data: Optional[Dict] = None) -> None:
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
    RESULT_PATH = '/tmp'
    TEST_RATIO = 0.2
    TRIM_RATIO = 0.2
    EXPOSE_ALL = True
    TXN_SAMPLE_INTERVAL = 49

    def __init__(self, end_point:str) -> ModelServer:
        """
        Initialize the ModelServer by connecting to the ZMQ IPC endpoint
        :param end_point:  IPC endpoint
        """
        # Establish ZMQ connection
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.set_string(zmq.IDENTITY, 'model')
        logging.debug(f"Python model trying to connect to manager at {end_point}")
        self.socket.connect(f"ipc://{end_point}")
        logging.debug(f"Python model connected at {end_point}")

        # Register the exit callback
        atexit.register(self.cleanup_zmq)

        # Gobal model map cache
        self.g_model_map = None

        # Notify the ModelServerManager that I am connected
        self._send_msg(0, 0, ModelServer._make_response(Callback.CONNECTED, ""))

    def cleanup_zmq(self):
        """
        Close the socket when the script exits
        :return:
        """
        self.socket.close()
        self.context.destroy()


    def _send_msg(self, send_id: int, recv_id: int, data:Dict) ->None:
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
    def _make_response(action: Callback, result: Any) -> Dict:
        """
        Construct a response to the ModelServerManager
        :param action:  Action callback on the ModelServerManager
        :param result: Any result
        :return:
        """
        return {
            "action": action,
            "result": result
        }

    @staticmethod
    def _parse_msg(payload:str) -> Tuple[int, int, Message]:
        logging.debug("PY RECV: " + payload)
        tokens = payload.split('-', 2)

        # Invalid message format
        try:
            msg_id = int(tokens[0])
            recv_id = int(tokens[1])
        except ValueError as e:
            logging.error(f"Invalid message payload format: {payload}, ids not int.")
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
        return [opunit_data.OpUnitData(OpUnit(x[0]), x[1][0], x[1][1]) for x in raw_data]

    def _train_model(self, data: Dict) -> str:
        """
        Train a model with the given model name and seq_files directory
        :param data: {
            models: [lr, XXX, ...],
            seq_files: PATH_TO_SEQ_FILES_FOLDER, or None
            data_lists: [(OpUnit, X_List, Y_List)/OpUnitData]
        }
        :return: the model map
        """
        ml_models = data["models"]
        seq_files_dir = data["seq_files"]
        raw_data = data["raw_data"]

        result_path = ModelServer.RESULT_PATH
        test_ratio = ModelServer.TEST_RATIO
        trim = ModelServer.TRIM_RATIO
        expose_all = ModelServer.EXPOSE_ALL
        txn_sample_interval = ModelServer.TXN_SAMPLE_INTERVAL
        trainer = MiniTrainer(seq_files_dir, result_path, ml_models, test_ratio, trim, expose_all,txn_sample_interval)
        if seq_files_dir:
            # Perform training from MiniTrainer and input files directory
            model_map = trainer.train()
        else:
            """
            NOTE:
            This branch has not been tested yet because the MiniTrainer now heavily relies on the CSV file. 
            So training with data directly is not yet possible without refactoring the MiniTrainer 
            """

            # Train from data directly
            assert(raw_data)

            # Transform with raw data lists
            opunit_data_list = self._transform_opunit_data(raw_data)
            for data in opunit_data_list:
                best_y_transformer, best_method = trainer.train_data(data, None)
                if trainer.expose_all:
                    trainer.train_specific_model(data, best_y_transformer, best_method)

            # Directly get the model map
            model_map = trainer.get_model_map()

        self.g_model_map = model_map

        return "SUCCESS"


    def _infer(self, data:Dict) -> Optional[Dict]:
        """
        Do inference on the model, give the data file, and the model_map_path
        :param data: {
            features: 2D float arrays [[float]],
            opunit: Opunit integer for the model
        }
        :return: List predictions
        """
        features = data["features"]
        opunit = data["opunit"]

        # Parameter validation
        if not isinstance(opunit, str):
            return None

        features = np.array(features)
        logging.info(f"Using model on {opunit}")
        # Load the model map
        if self.g_model_map is None:
            logging.error("Model has not been trained")
            return None
        model = self.g_model_map[OpUnit[opunit.upper()]]
        y_pred = model.predict(features)

        return y_pred


    def _execute_cmd(self,cmd: Command, data: Dict) -> Tuple[Dict, Boolean]:
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
            response = self._make_response(Callback.NOOP, f"MODEL_REPLY_{msg}")
            return response, True
        elif cmd == Command.QUIT:
            # Will not send any message so empty {} is ok
            return self._make_response(Callback.NOOP, ""), False
        elif cmd == Command.TRAIN:
            try:
                res = self._train_model(data)
                response = self._make_response(Callback.NOOP, res)
            except ValueError as e:
                logging.error(f"Model Not found : {e}")
                response = self._make_response(Callback.NOOP, "FAIL_MODEL_NOT_FOUND")
            except KeyError as e:
                logging.error(f"Data format wrong for TRAIN: {e}")
                response = self._make_response(Callback.NOOP, "FAIL_DATA_FORMAT_ERROR")
            except Exception as e:
                logging.error(f"Training failed. {e}")
                response = self._make_response(Callback.NOOP, "FAIL_TRAINING_FAILED")

            return response, True
        elif cmd == Command.INFER:
            result = self._infer(data)
            response = self._make_response(Callback.NOOP, result.tolist())
            return response, True

    def run_loop(self):
        """
        Run in a loop to recv/send message to the ModelServer manager
        :return:
        """

        while(1):
            identity = self.socket.recv()
            _delim = self.socket.recv()
            payload = self.socket.recv()
            logging.debug(f"Python recv: {str(identity)}, {str(payload)}")

            payload = payload.decode("ascii")
            send_id, recv_id, msg = self._parse_msg(payload)
            if msg is None:
                continue
            else:
                result, cont = self._execute_cmd(msg.cmd, msg.data)
                if not cont:
                    logging.info("Shutting down.")
                    break

                # Currently not expecting to invoke any callback on ModelServer side, so second parameter 0
                self._send_msg(0, send_id, result)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ./model_server.py <ZMQ_ENDPOINT>")
        exit(-1)
    ms = ModelServer(sys.argv[1])
    ms.run_loop()


