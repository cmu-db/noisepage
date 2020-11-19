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
- ModelServer side of callbacks (once we are more clear on the actions)
- Model invocation could be improved.

"""

from __future__ import annotations
import sys
import atexit
from enum import Enum, auto
from typing import Dict, Optional, Tuple
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


logging_util.init_logging('info')
# Initialize ZMQ connection with the c++ side
end_point = sys.argv[1]
context = zmq.Context()
socket = context.socket(zmq.DEALER)
socket.set_string(zmq.IDENTITY, 'model')
logging.info(f"Python model trying to connect to manager at {end_point}")
socket.connect(f"ipc://{end_point}")
logging.info(f"Python model connected at {end_point}")


def cleanup_zmq():
    """
    Close the socket when the script exits
    :return:
    """
    socket.close()
    context.destroy()

# Register the exit callback
atexit.register(cleanup_zmq)


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


def parse_msg(payload:str) -> Tuple[int, int, Message]:
    logging.debug("PY RECV: " + payload)
    tokens = payload.split('-')

    # Invalid message format
    if len(tokens) != 3:
        logging.error(f"Invalid message payload format: {payload}")
        return -1, -1, None
    try:
        msg_id = int(tokens[0])
        recv_id = int(tokens[1])
    except ValueError as e:
        logging.error(f"Invalid message payload format: {payload}")
        return -1, -1, None

    msg = Message.from_json(tokens[2])
    return msg_id, recv_id, msg

def train_model(data: Dict) -> str:
    """
    Train a model with the given model name and seq_files directory
    :param data: {
        model_name: lr,
        seq_files: PATH_TO_SEQ_FILES_FOLDER
    }
    :return: str where the model map is saved
    """
    model_name = data["model_name"]
    seq_files_dir = data["seq_files"]

    # TODO(ricky): capture parameters with a config class?
    # perform training
    result_path = "/tmp"
    ml_models = [model_name]
    test_ratio = 0.2
    trim = 0.2
    expose_all = True
    trainer = MiniTrainer(seq_files_dir, result_path, ml_models, test_ratio, trim, expose_all)
    model_map = trainer.train()

    # Save model
    save_path = f"/tmp/{model_name}.pickle"
    with open(save_path, 'wb') as file:
        pickle.dump(model_map, file)
    return save_path


def test_model(data:Dict) -> None:
    """
    Do inference on the model, give the data file, and the model_map_path
    :param data: {
        data_file: PATH_TO_SEQ_FILE ,
        model_map_path: PATH_TO_PICKLE_FILE
    }
    :return: List Test errors
    """
    data_file = data["data_file"]
    model_map_path = data["model_map_path"]

    result_path = ""
    test_ratio = 0.9
    trim = 0.2
    error_bias = 1
    data = opunit_data.get_mini_runner_data(data_file, result_path, {}, {}, trim)[0]
    x_train, x_test, y_train, y_test = model_selection.train_test_split(data.x, data.y,
                                                                        test_size=test_ratio,
                                                                        random_state=0)
    # Load the model map
    with open(model_map_path, 'rb') as f:
        model_map = pickle.load(f)

    results = []
    for opunit, model in model_map.items():
        y_pred = model.predict(x_test)

        weights = np.where(y_test > 5, np.ones(y_test.shape), np.full(y_test.shape, 1e-6))
        percentage_error = np.average(np.abs(y_test - y_pred) / (y_test + error_bias), axis=0,
                                      weights=weights)
        results += list(percentage_error) + [""]

    return results


def send_msg(socket, send_id, recv_id, data) :
    """
    Send a message to the socket.
    :param socket: underlying socket
    :param send_id: id on this end, 0 for now
    :param recv_id: callback id to invoke on the other end
    :param data: payload of the message in JSON
    :return:
    """
    # socket.send(b"", flags=zmq.SNDMORE)
    msg = f"{send_id}-{recv_id}-{data}"
    socket.send_multipart([''.encode('utf-8'), msg.encode('utf-8')])


# Loop until quit
def run_loop():
    """
    Run in a loop to recv/send message to the ModelServer manager
    :return:
    """
    while(1):
        identity = socket.recv()
        delim = socket.recv()
        payload = socket.recv()
        logging.info(f"Python recv: {str(identity)}, {str(payload)}")

        payload = payload.decode("ascii")
        send_id, recv_id, msg = parse_msg(payload)
        if msg is None:
            continue
        elif msg.cmd == Command.PRINT:
            logging.info(f"MESSAGE PRINT: {str(msg)}")
            send_msg(socket, 0, send_id, str(msg))
        elif msg.cmd == Command.QUIT:
            logging.info("Quiting")
            break
        elif msg.cmd == Command.TRAIN:
            try:
                model_path = train_model(msg.data)
                logging.info(f"Model_path: {model_path}")
                send_msg(socket, 0, send_id, model_path)
            except ValueError as e:
                logging.error(f"Model Not found : {msg.data}")
            except KeyError as e:
                logging.error(f"Data format wrong for TRAIN")
        elif msg.cmd == Command.INFER:
            errors = test_model(msg.data)
            logging.info(f"Test errors: {errors}")

run_loop()
logging.info("Model shutting down")


