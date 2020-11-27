#!/usr/bin/env python3
"""
This file contains the python ModelServer implementation.

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
from typing import Dict, Optional, Tuple, List
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
# Initialize ZMQ connection with the c++ side
end_point = sys.argv[1]
context = zmq.Context()
socket = context.socket(zmq.DEALER)
socket.set_string(zmq.IDENTITY, 'model')
logging.info(f"Python model trying to connect to manager at {end_point}")
socket.connect(f"ipc://{end_point}")
logging.info(f"Python model connected at {end_point}")

# Gobal model map cache
g_model_map = None


def cleanup_zmq():
    """
    Close the socket when the script exits
    :return:
    """
    socket.close()
    context.destroy()

# Register the exit callback
atexit.register(cleanup_zmq)


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


def _train_model(data: Dict) -> Dict:
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

    result_path = "/tmp"
    test_ratio = 0.2
    trim = 0.2
    expose_all = True
    trainer = MiniTrainer(seq_files_dir, result_path, ml_models, test_ratio, trim, expose_all)
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
        opunit_data_list = _transform_opunit_data(raw_data)
        for data in opunit_data_list:
            best_y_transformer, best_method = trainer.train_data(data, None)
            if trainer.expose_all:
                trainer.train_specific_model(data, best_y_transformer, best_method)

        # Directly get the model map
        model_map = trainer.get_model_map()

    return model_map


def _infer(data:Dict) -> Optional[Dict]:
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
    # Load the model map
    if g_model_map is None:
        logging.error("Model has not been trained")
        return None
    model = g_model_map[OpUnit[opunit.upper()]]
    y_pred = model.predict(features)

    return y_pred


def _send_msg(socket, send_id, recv_id, data) :
    """
    Send a message to the socket.
    :param socket: underlying socket
    :param send_id: id on this end, 0 for now
    :param recv_id: callback id to invoke on the other end
    :param data: payload of the message in JSON
    :return:
    """
    json_result = json.dumps(data)
    msg = f"{send_id}-{recv_id}-{json_result}"
    socket.send_multipart([''.encode('utf-8'), msg.encode('utf-8')])


def _execute_cmd(cmd: Command, data: Dict) -> Tuple[Dict, Boolean]:
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
        response = {
            "result": f"MODEL_REPLY_{msg}",
            "action": Callback.NOOP
        }
        return response, True
    elif cmd == Command.QUIT:
        # Will not send any message so empty {} is ok
        return {}, False
    elif cmd == Command.TRAIN:
        try:
            model_map = _train_model(data)
            global g_model_map
            g_model_map = model_map
            logging.info(f"Model_path: {model_map}")
            response = {
                "result": "SUCCESS",
                "action": Callback.NOOP

            }
        except ValueError as e:
            logging.error(f"Model Not found : {e}")
            response = {
                "result": "FAIL_MODEL_NOT_FOUND",
                "action": Callback.NOOP
            }
        except KeyError as e:
            logging.error(f"Data format wrong for TRAIN: {e}")
            response = {
                "result": "FAIL_DATA_FORMAT_ERROR",
                "action": Callback.NOOP
            }
        except Exception as e:
            logging.error(f"Training failed. {e}")
            response = {
                "result": "FAIL_TRAINING_FAILED",
                "action": Callback.NOOP
            }

        return response, True
    elif cmd == Command.INFER:
        result = _infer(data)
        response = {
            "result": result.tolist(),
            "action": Callback.NOOP
        }
        return response, True



# Loop until quit
def run_loop():
    """
    Run in a loop to recv/send message to the ModelServer manager
    :return:
    """

    # ModelServer Connected
    _send_msg(socket, 0, 0, {"action": Callback.CONNECTED, "result": ""})
    while(1):
        identity = socket.recv()
        delim = socket.recv()
        payload = socket.recv()
        logging.debug(f"Python recv: {str(identity)}, {str(payload)}")

        payload = payload.decode("ascii")
        send_id, recv_id, msg = _parse_msg(payload)
        if msg is None:
            continue
        else:
            result, cont = _execute_cmd(msg.cmd, msg.data)
            if not cont:
                logging.info("Shutting down.")
                break

            # Currently not expecting to invoke any callback on ModelServer side, so second parameter 0
            _send_msg(socket, 0, send_id, result)


run_loop()
logging.info("Model shut down")


