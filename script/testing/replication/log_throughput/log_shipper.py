import atexit
from threading import Thread
from typing import List

import zmq
from zmq import Socket


class LogShipper:
    def __init__(self, log_file: str, primary_identity: str, primary_messenger_port: int, primary_replication_port: int,
                 replica_identity: str, replica_replication_port: int):
        self.running = True
        self.identity = primary_identity

        with open(log_file, 'r') as f:
            self.messages = f.readlines()
            self.messages = [message.strip() for message in self.messages]

        self.context = zmq.Context()

        # Default socket bound to the messenger port
        # Probably not needed
        self.default_socket = self.context.socket(zmq.ROUTER)
        self.default_socket.set_string(zmq.IDENTITY, primary_identity)
        self.default_socket.bind(f"tcp://*:{primary_messenger_port}")
        self.default_socket.bind(f"ipc://./noisepage-ipc-{primary_messenger_port}")
        self.default_socket.bind(f"inproc://noisepage-inproc-{primary_messenger_port}")

        # Replica connection used to send messages to replica
        self.replica_dealer_socket = self.context.socket(zmq.DEALER)
        self.replica_dealer_socket.set_string(zmq.IDENTITY, replica_identity)
        self.replica_dealer_socket.connect(f"tcp://127.0.0.1:{replica_replication_port}")

        # Create thread to receive and ack messages so the replica doesn't get backed up
        self.recv_context = None
        self.primary_router_socket = None
        self.recv_thread = Thread(target=self.recv_thread_action, args=(primary_replication_port,))
        self.recv_thread.start()

        atexit.register(self.cleanup_zmq)

    def ship(self):
        """
        Send log records to replica
        """
        for message in self.messages:
            self.send_log_record(message)
            # Check and dispose of ACKs
            if self.has_pending_messages(self.replica_dealer_socket, 1):
                self.recv_ack(self.replica_dealer_socket)

    def send_log_record(self, log_record_message: str):
        """
        Send log record message to replica

        :param log_record_message Log record to send
        """
        self.send_msg(["", log_record_message], self.replica_dealer_socket)

    def send_ack_msg(self, message_id: str, socket: Socket):
        """
        Sends ack message

        :param message_id ID of message that we are ACKing
        :param socket Socket to send ACK over
        """
        self.send_msg([self.identity, "", f"{message_id}-0-2-"], socket)

    @staticmethod
    def send_msg(message_parts: List[str], socket: Socket):
        """
        Send multipart message over socket

        :param message_parts messages to send
        :param socket socket to send over
        """
        socket.send_multipart([message.encode('utf-8') for message in message_parts])

    def recv_thread_action(self, primary_replication_port: int):
        """
        Set up context for receiving messages and then continuously receive messages until the log shipper is done.
        This is just so messages from the replica don't build up in the queue and the test can be more realistic

        :param primary_replication_port Replication port of primary that we're imitating
        """
        self.recv_context = zmq.Context()
        # Primary replication socket that listens for messages from the replica
        self.primary_router_socket = self.context.socket(zmq.ROUTER)
        self.primary_router_socket.set_string(zmq.IDENTITY, self.identity)
        self.primary_router_socket.bind(f"tcp://127.0.0.1:{primary_replication_port}")
        while self.running:
            if self.has_pending_messages(self.primary_router_socket, 100):
                self.recv_txn_applied_msg()
        self.primary_router_socket.close()
        self.recv_context.destroy()

    def recv_txn_applied_msg(self):
        """
        Receive txn applied message from replica and ACK back
        """
        identity = self.recv_msg(self.primary_router_socket)
        empty_msg = self.recv_msg(self.primary_router_socket)
        msg = self.recv_msg(self.primary_router_socket)
        msg_id = str(msg).split("-")[0]
        self.send_ack_msg(msg_id, self.primary_router_socket)

    def recv_ack(self, socket: Socket):
        """
        Receives an ack. We don't care about the contents so we just throw it away

        :param socket Socket to receive ACK on
        """
        receiver_identity = self.recv_msg(socket)
        empty_message = self.recv_msg(socket)
        ack_message = self.recv_msg(socket)

    @staticmethod
    def recv_msg(socket: Socket) -> str:
        """
        Receive message from socket

        :param socket Socket to receive message from
        """
        return socket.recv()

    @staticmethod
    def has_pending_messages(socket: Socket, timeout: int) -> bool:
        """
        Checks if a socket has any pending messages

        :param socket Socket to check for messages
        :param timeout How long to check for messages

        :return True if there are pending messages, false otherwise
        """
        return socket.poll(timeout) == zmq.POLLIN

    def cleanup_zmq(self):
        """
        Close the socket and context when the script exits
        """
        self.running = False
        self.recv_thread.join()
        self.replica_dealer_socket.close()
        self.default_socket.close()
        self.context.destroy()
