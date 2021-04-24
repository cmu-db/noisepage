from threading import Thread

import zmq

from .node_server import ImposterNode


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
        self.sink_context = zmq.Context()
        self._create_receiving_router_socket(self.sink_context)
        self.primary_dealer_socket = self._create_sending_dealer_socket(self.sink_context, self.primary_identity,
                                                                        self.primary_replication_port)
        with open(self.log_file, 'w') as f:
            while self.is_running():
                if self.has_pending_messages(self.router_socket, 100):
                    log_record_msg = self.recv_log_record()
                    f.write(f"{log_record_msg}\n")
                    msg_id = self.extract_msg_id(log_record_msg)
                    self.send_ack_msg(msg_id, self.router_socket)

            # Drain any additional messages
            while self.has_pending_messages(self.router_socket, 2000):
                log_record_msg = self.recv_log_record()
                f.write(f"{log_record_msg}\n")
                msg_id = self.extract_msg_id(log_record_msg)
                self.send_ack_msg(msg_id, self.router_socket)

        self.teardown_router_socket()
        self.primary_dealer_socket.close()
        self.sink_context.destroy()

    def recv_log_record(self) -> str:
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
