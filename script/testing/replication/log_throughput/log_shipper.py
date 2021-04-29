from threading import Thread
from time import sleep
from typing import BinaryIO

import zmq
from zmq import Socket

from .constants import SIZE_LENGTH, ENDIAN
from .node_server import ImposterNode
from ...util.constants import LOG


class LogShipper(ImposterNode):
    """
    Helper class to send log record messages to replica nodes
    """

    def __init__(self, log_file: str, primary_identity: str, primary_messenger_port: int, primary_replication_port: int,
                 replica_identity: str, replica_replication_port: int):
        """
        Initializes LogShipper

        Parameters
        ----------
        log_file
            file contain log record messages
        primary_identity
            network identity of primary node
        primary_messenger_port
            port that the primary messenger runs on
        primary_replication_port
            port that the primary replication runs on
        replica_identity
            network identity of replica node
        replica_replication_port
            port that the replica replication runs on
        """
        ImposterNode.__init__(self, primary_identity, primary_messenger_port, primary_replication_port)

        self.log_file = log_file
        self.log_file_len = 0

        # Replica connection used to send messages to replica
        self.replica_dealer_socket = self._create_sending_dealer_socket(self.context, replica_identity,
                                                                        replica_replication_port)

        self.pending_log_msgs = {}

        # Create thread to receive and ack messages so the replica doesn't get backed up
        self.recv_context = None
        self.recv_thread = Thread(target=self.recv_thread_action)
        self.recv_thread.start()

    def setup(self):
        self.log_file_len = 0
        for _ in self.read_messages():
            self.log_file_len += 1

    def run(self):
        self.ship()

    def ship(self):
        """
        Send log records to replica.
        This method guarantees that the replica node receives all log messages, however it does not guarantee that the
        replica has applied all transactions sent. It attempts to wait for the replica to apply all transactions by
        waiting for a gap longer than 5 seconds in between TXN APPLIED messages (see recv_thread_action()). If we need
        something more precise then you can keep track of the TXN APPLIED messages and block until all transactions that
        were sent have been applied.
        """
        LOG.info("Shipping logs to replica")

        for idx, message in enumerate(self.read_messages()):
            # Check and dispose of ACKs
            if self.has_pending_messages(self.replica_dealer_socket, 0):
                self.recv_log_record_ack()

            self.send_log_record(message)

            # Every thousand messages log status and retry any pending messages
            # This is a bit naive, but we want to ship logs as fast as possible and not spend too long every
            # iteration checking for dropped messages
            if idx % 1000 == 0 and idx != 0:
                LOG.info(f"Shipping log number {idx} out of {self.log_file_len}")
                self.retry_pending_msgs()

        LOG.info("Waiting for replica to ACK all messages")

        while self.pending_log_msgs:
            self.retry_pending_msgs()
            sleep(10)

        LOG.info("Log shipping has completed")

    def read_messages(self):
        """
        Reads in messages from log file
        """
        with open(self.log_file, "rb") as f:
            size_bytes = f.read(SIZE_LENGTH)
            while size_bytes:
                size = int.from_bytes(size_bytes, ENDIAN)
                yield f.read(size)
                size_bytes = f.read(SIZE_LENGTH)

    def retry_pending_msgs(self):
        """
        Retry all pending messages
        """
        # Drain ACKs
        while self.has_pending_messages(self.replica_dealer_socket, 0):
            self.recv_log_record_ack()
        for pending_message in self.pending_log_msgs.values():
            self.send_msg([b"", pending_message], self.replica_dealer_socket)

    def send_log_record(self, log_record_message: bytes):
        """
        Send log record message to replica

        Parameters
        ----------
        log_record_message
            Log record to send (can also be a Notify OAT message)
        """
        self.send_msg([b"", log_record_message], self.replica_dealer_socket)
        msg_id = self.extract_msg_id(log_record_message)
        self.pending_log_msgs[msg_id] = log_record_message

    def recv_thread_action(self):
        """
        Set up context for receiving messages and then continuously receive messages until the log shipper is done.
        This is just so messages from the replica don't build up in the queue and the test can be more realistic
        """
        self.recv_context = zmq.Context()
        # Create primary replication socket that listens for messages from the replica
        self._create_receiving_router_socket(self.recv_context)
        while self.running:
            if self.has_pending_messages(self.router_socket, 1):
                self.recv_txn_applied_msg()

        # Wait for all txn applied messages
        LOG.info("Waiting for replica to apply all transaction")
        while self.has_pending_messages(self.router_socket, 60000):
            self.recv_txn_applied_msg()
        self.teardown_router_socket()
        self.recv_context.destroy()

    def recv_txn_applied_msg(self):
        """
        Receive txn applied message from replica and ACK back
        """
        identity = self.recv_msg(self.router_socket)
        empty_msg = self.recv_msg(self.router_socket)
        msg = self.recv_msg(self.router_socket)
        msg_id = self.extract_msg_id(msg)
        self.send_ack_msg(msg_id, self.router_socket)

    def recv_log_record_ack(self):
        """
        Receives an ACK for a log record and update pending log records
        """
        acked_msg_id = self.recv_ack(self.replica_dealer_socket)
        if acked_msg_id in self.pending_log_msgs:
            self.pending_log_msgs.pop(acked_msg_id)

    def recv_ack(self, socket: Socket) -> bytes:
        """
        Receives an ACK.

        Parameters
        ----------
        socket
            Socket to receive ACK on

        Returns
        -------
        msg_id
            Message Id of ACK
        """
        receiver_identity = self.recv_msg(socket)
        empty_message = self.recv_msg(socket)
        ack_message = self.recv_msg(socket)
        return self.extract_msg_id(ack_message)

    def teardown(self):
        """
        Close the socket and context when the script exits
        """
        self.replica_dealer_socket.close()
        ImposterNode.teardown(self)
        self.recv_thread.join()
