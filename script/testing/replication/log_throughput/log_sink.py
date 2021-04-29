import os
from threading import Thread
from typing import BinaryIO

import zmq

from .constants import ENDIAN, SIZE_LENGTH
from .node_server import ImposterNode
from ...util.constants import LOG


class LogSink(ImposterNode):
    """
    Helper class to capture and save log record messages
    """

    def __init__(self, log_file: str, replica_identity: str, replica_messenger_port: int,
                 replica_replication_port: int, primary_identity: str, primary_replication_port: int):
        """
        Initialize LogSink

        Parameters
        ----------
        log_file
            File to save log record messages to
        replica_identity
            network identity of replica node
        replica_messenger_port
            port that the replica messenger runs on
        replica_replication_port
            port that replica replication runs on
        primary_identity
            network identity of primary node
        primary_replication_port
            port that primary replication runs on
        """
        ImposterNode.__init__(self, replica_identity, replica_messenger_port, replica_replication_port)
        self.log_file = log_file
        self.primary_identity = primary_identity
        self.primary_replication_port = primary_replication_port
        self.sink_context = None
        self.primary_dealer_socket = None
        self.sink_thread = None

    def setup(self):
        """
        Start thread to listen for and save messages
        """
        self.sink_thread = Thread(target=self.sink)
        self.sink_thread.start()

    def sink(self):
        """
        While running continuously receive log record messages and save them to a file
        """

        if os.path.exists(self.log_file):
            os.remove(self.log_file)

        self.sink_context = zmq.Context()
        self._create_receiving_router_socket(self.sink_context)
        self.primary_dealer_socket = self._create_sending_dealer_socket(self.sink_context, self.primary_identity,
                                                                        self.primary_replication_port)
        with open(self.log_file, 'wb') as f:
            while self.is_running():
                if self.has_pending_messages(self.router_socket, 100):
                    self.save_message(f)

            # Drain any additional messages
            LOG.info("Waiting for all messages to arrive")
            while self.has_pending_messages(self.router_socket, 2000):
                self.save_message(f)

        self.teardown_router_socket()
        self.primary_dealer_socket.close()
        self.sink_context.destroy()

    def save_message(self, f: BinaryIO):
        """
        Save message to file. For each message we first write the length of the message using an int and then write the
        message itself. The reason for this is that the messages themselves are binary and we have no good delimiter
        like newlines we can put between them.

        Parameters
        ----------
        f
            file to save message to
        """
        log_record_msg = self.recv_log_record()
        size: int = len(log_record_msg)
        f.write(size.to_bytes(SIZE_LENGTH, ENDIAN))
        f.write(log_record_msg)
        msg_id = self.extract_msg_id(log_record_msg)
        self.send_ack_msg(msg_id, self.router_socket)

    def recv_log_record(self) -> bytes:
        """
        Receive a log record message from the primary node (can also be a Notify OAT message)

        Returns
        -------
        log_record_msg
            log record message from primary node
        """
        identity = self.recv_msg(self.router_socket)
        empty_msg = self.recv_msg(self.router_socket)
        return self.recv_msg(self.router_socket)

    def run(self):
        pass

    def teardown(self):
        """
        Close socket, destroy contexts, and join on sink thread
        """
        ImposterNode.teardown(self)
        self.sink_thread.join()
