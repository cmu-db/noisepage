import os.path
import re
from abc import abstractmethod, ABC
from enum import Enum
from typing import List, Union

import zmq

from .constants import *
from .test_type import TestType
from ...oltpbench.constants import OLTPBENCH_GIT_LOCAL_PATH
from ...oltpbench.test_case_oltp import TestCaseOLTPBench
from ...oltpbench.test_oltpbench import TestOLTPBench
from ...util.db_server import NoisePageServer


class NodeServer(ABC):
    """
    Represents an instance of a NoisePage Server along with anything it needs for log throughput testing
    """

    @abstractmethod
    def setup(self):
        """
        Setup server
        """
        pass

    @abstractmethod
    def run(self):
        """
        Generate and process log records
        """
        pass

    @abstractmethod
    def teardown(self):
        """
        Tear down server
        """
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """
        Indicates whether or not the server is running

        :return True if the server is running False otherwise
        """
        pass


class PrimaryNode(NodeServer):
    """
    Primary node NoisePage server. Generates log records by running the loading phase of an OLTP benchmark
    """

    def __init__(self, build_type: str, replication_enabled: bool, async_replication: bool,
                 async_commit: bool, oltp_benchmark: str, scale_factor: int, connection_threads: int):
        """
        Creates the NoisePage primary server and an OLTP test case

        :param build_type build type of NoisePage binary
        :param replication_enabled Whether or not to enable replication
        :param async_replication Whether or not async replication is enabled (only relevant when replication is enabled)
        :param async_commit
        :param oltp_benchmark Which OLTP benchmark to run
        :param scale_factor OLTP benchmark scale factor
        :param connection_threads How many database connection threads to use
        """
        # Create DB instance
        primary_server_args = DEFAULT_PRIMARY_SERVER_ARGS
        primary_server_args[BUILD_TYPE_KEY] = build_type
        primary_server_args[SERVER_ARGS_KEY][WAL_ASYNC_COMMIT_KEY] = async_commit
        primary_server_args[SERVER_ARGS_KEY][CONNECTION_THREAD_COUNT_KEY] = connection_threads
        if replication_enabled:
            primary_server_args[SERVER_ARGS_KEY][MESSENGER_ENABLED_KEY] = True
            primary_server_args[SERVER_ARGS_KEY][REPLICATION_ENABLED_KEY] = True
            primary_server_args[SERVER_ARGS_KEY][ASYNC_REPLICATION_KEY] = async_replication
        self.oltp_server = TestOLTPBench(primary_server_args)

        # Create OLTP test case
        oltp_test_case = DEFAULT_OLTP_TEST_CASE
        oltp_test_case[BENCHMARK_KEY] = oltp_benchmark
        oltp_test_case[WEIGHTS_KEY] = WEIGHTS_MAP[oltp_benchmark]
        oltp_test_case[TERMINALS_KEY] = connection_threads
        oltp_test_case[LOADER_THREADS_KEY] = connection_threads
        oltp_test_case[SCALE_FACTOR_KEY] = scale_factor
        self.test_case = TestCaseOLTPBench(oltp_test_case)

        self.running = False

    def setup(self):
        """
        Start the primary NoisePage node and download and compile OLTP Bench
        """
        # Start DB
        if not self.oltp_server.db_instance.run_db(timeout=30):
            raise RuntimeError("Unable to start database")
        self.running = True
        # Download and prepare OLTP Bench
        self.oltp_server._clean_oltpbench()
        self.oltp_server._download_oltpbench()
        self.overwrite_ycsb_field_size()
        self.oltp_server._build_oltpbench()

    @staticmethod
    def overwrite_ycsb_field_size():
        """
        Overwrites the YCSB constants file to replace FIELD_SIZE with 1
        """
        ycsb_constants_file_path = os.path.join(
            *[OLTPBENCH_GIT_LOCAL_PATH, "src", "com", "oltpbenchmark", "benchmarks", "ycsb", "YCSBConstants.java"])
        new_constants_file = ""
        with open(ycsb_constants_file_path, 'r') as f:
            for line in f.readlines():
                if "FIELD_SIZE" in line:
                    line = re.sub(r"\d+", "1", line)
                new_constants_file += line
        with open(ycsb_constants_file_path, 'w') as f:
            f.write(new_constants_file)

    def run(self):
        """
        Run the loading phase of the OLTP Benchmark
        """
        # Load DB
        self.test_case.run_pre_test()

    def teardown(self):
        """
        Stop the primary node and delete it's WAL
        """
        self.oltp_server.db_instance.stop_db()
        self.running = False
        self.oltp_server.db_instance.delete_wal()

    def is_running(self) -> bool:
        return self.running


class ReplicaNode(NodeServer):
    """
    Replica node NoisePage server. Generates log records by manually sending pre-collected messages
    """

    def __init__(self, test_type: TestType, build_type: str, async_commit: bool, log_messages_file: str,
                 connection_threads: int):
        """
        Creates the NoisePage replica server. If we are testing throughput on the replica node then we also create a
        log shipper instance to send logs to the replica

        :param test_type Indicates whether we are testing throughput on the primary or replica node
        :param build_type Build type of NoisePage binary
        :param log_messages_file File containing log record messages to send to the replica
        :param connection_threads How many database connection threads to use
        """
        replica_server_args = DEFAULT_REPLICA_SERVER_ARGS
        replica_server_args[SERVER_ARGS_KEY][WAL_ASYNC_COMMIT_KEY] = async_commit
        replica_server_args[SERVER_ARGS_KEY][CONNECTION_THREAD_COUNT_KEY] = connection_threads

        self.ship_logs = False
        if test_type == TestType.REPLICA:
            replica_server_args[SERVER_ARGS_KEY][METRICS_KEY] = True
            replica_server_args[SERVER_ARGS_KEY][USE_METRICS_THREAD_KEY] = True
            replica_server_args[SERVER_ARGS_KEY][LOGGING_METRICS_ENABLED_KEY] = True
            self.ship_logs = True

        # Create DB instance
        replica_server_args[BUILD_TYPE_KEY] = build_type
        self.replica = NoisePageServer(build_type=build_type,
                                       port=replica_server_args[SERVER_ARGS_KEY][PORT_KEY],
                                       server_args=replica_server_args[SERVER_ARGS_KEY])

        self.running = False

    def setup(self):
        """
        Start the replica node
        """
        # Start DB
        if not self.replica.run_db(timeout=30):
            raise RuntimeError("Unable to start database")
        self.running = True

    def run(self):
        pass

    def teardown(self):
        """
        Shut down the log shipper if needed and stop the replica node and delete it's WAL
        """
        self.replica.stop_db()
        self.running = False
        self.replica.delete_wal()

    def is_running(self) -> bool:
        return self.running


class ImposterNode(NodeServer):
    """
    Python process that imitates a NoisePage server. This is useful for mocking one side of replication
    """

    def __init__(self, identity: str, messenger_port: int, replication_port: int):
        """
        Initializes ImposterNode

        :param identity network identity of node
        :param messenger_port port that messenger is running on
        :param replication_port port that replication is running on
        """
        self.running = True
        self.identity = identity
        self.replication_port = replication_port

        self.context = zmq.Context()
        # Default socket bound to the messenger port
        self.default_socket = self.context.socket(zmq.ROUTER)
        self.default_socket.set_string(zmq.IDENTITY, identity)
        self.default_socket.bind(f"tcp://*:{messenger_port}")
        self.default_socket.bind(f"ipc://./noisepage-ipc-{messenger_port}")
        self.default_socket.bind(f"inproc://noisepage-inproc-{messenger_port}")

        # Router socket used to receive messages
        self.router_socket = None

    def _create_receiving_router_socket(self, context: zmq.Context):
        """
        Creates the socket responsible for receiving messages. This method allows the flexibility of creating this
        socket in the main thread or a different thread.

        WARNING:
        You must only call this method once and you must call this from the same thread that context was created in.
        You must also make sure to destroy the socket properly, from the same thread that created it

        :param context ZMQ context to use to create the socket
        """
        self.router_socket = context.socket(zmq.ROUTER)
        self.router_socket.set_string(zmq.IDENTITY, self.identity)
        self.router_socket.setsockopt(zmq.LINGER, 0)
        self.router_socket.bind(f"tcp://127.0.0.1:{self.replication_port}")

    @staticmethod
    def _create_sending_dealer_socket(context: zmq.Context, connection_identity: str,
                                      connection_replication_port) -> zmq.Socket:
        """
        Creates a socket for sending messages to another node.

        WARNING:
        You must call this from the same thread the context was created in.

        :param context ZMQ context to use to create the socket
        """
        dealer_socket = context.socket(zmq.DEALER)
        dealer_socket.set_string(zmq.IDENTITY, connection_identity)
        dealer_socket.setsockopt(zmq.LINGER, 0)
        dealer_socket.connect(f"tcp://127.0.0.1:{connection_replication_port}")
        return dealer_socket

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @staticmethod
    def send_msg(message_parts: List[str], socket: zmq.Socket):
        """
        Send multipart message over socket

        :param message_parts messages to send
        :param socket socket to send over
        """
        socket.send_multipart([message.encode(UTF_8) for message in message_parts])

    def send_ack_msg(self, message_id: str, socket: zmq.Socket):
        """
        Sends ack message

        :param message_id ID of message that we are ACKing
        :param socket Socket to send ACK over
        """
        self.send_msg([self.identity, "", f"{message_id}-{BuiltinCallback.NOOP.value}-{BuiltinCallback.ACK.value}-"],
                      socket)

    @staticmethod
    def recv_msg(socket: zmq.Socket) -> str:
        """
        Receive message from socket

        :param socket Socket to receive message from
        """
        return socket.recv().decode(UTF_8)

    @staticmethod
    def has_pending_messages(socket: zmq.Socket, timeout: int) -> bool:
        """
        Checks if a socket has any pending messages

        :param socket Socket to check for messages
        :param timeout How long to check for messages

        :return True if there are pending messages, false otherwise
        """
        return socket.poll(timeout) == zmq.POLLIN

    @staticmethod
    def extract_msg_id(msg: Union[str, bytes]) -> str:
        msg_str = msg.decode(UTF_8) if isinstance(msg, bytes) else msg
        return msg_str.split("-")[0]

    def teardown_router_socket(self):
        """
        Closes router socket. Must be called from the same thread that created it
        """
        self.router_socket.close()

    def teardown(self):
        """
        Destroy default ZMQ context and close messenger ZMQ socket
        """
        self.running = False
        self.default_socket.close()
        self.context.destroy()

    def is_running(self) -> bool:
        return self.running


class BuiltinCallback(Enum):
    NOOP = 0,
    ECHO = 1,
    ACK = 2
