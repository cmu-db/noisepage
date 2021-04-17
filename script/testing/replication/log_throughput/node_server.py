import os.path
import re
from abc import abstractmethod, ABC

from .constants import *
from .log_shipper import LogShipper
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


class PrimaryNode(NodeServer):
    """
    Primary node NoisePage server. Generates log records by running the loading phase of an OLTP benchmark
    """

    def __init__(self, build_type: str, replication_enabled: bool, async_commit: bool, oltp_benchmark: str,
                 connection_threads: int):
        """
        Creates the NoisePage primary server and an OLTP test case

        :param build_type build type of NoisePage binary
        :param replication_enabled Whether or not to enable replication
        :param oltp_benchmark Which OLTP benchmark to run
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
        self.oltp_server = TestOLTPBench(primary_server_args)

        # Create OLTP test case
        oltp_test_case = DEFAULT_OLTP_TEST_CASE
        oltp_test_case[BENCHMARK_KEY] = oltp_benchmark
        oltp_test_case[TERMINALS_KEY] = connection_threads
        oltp_test_case[LOADER_THREADS_KEY] = connection_threads
        self.test_case = TestCaseOLTPBench(oltp_test_case)

    def setup(self):
        """
        Start the primary NoisePage node and download and compile OLTP Bench
        """
        # Start DB
        if not self.oltp_server.db_instance.run_db(timeout=30):
            raise RuntimeError("Unable to start database")
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
        self.oltp_server.db_instance.delete_wal()


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

            primary_identity = DEFAULT_PRIMARY_SERVER_ARGS[SERVER_ARGS_KEY][NETWORK_IDENTITY_KEY]
            primary_messenger_port = DEFAULT_PRIMARY_SERVER_ARGS[SERVER_ARGS_KEY][MESSENGER_PORT_KEY]
            primary_replication_port = DEFAULT_PRIMARY_SERVER_ARGS[SERVER_ARGS_KEY][REPLICATION_PORT_KEY]
            replica_identity = replica_server_args[SERVER_ARGS_KEY][NETWORK_IDENTITY_KEY]
            replica_replication_port = replica_server_args[SERVER_ARGS_KEY][REPLICATION_PORT_KEY]
            self.log_shipper = LogShipper(log_messages_file, primary_identity, primary_messenger_port,
                                          primary_replication_port, replica_identity, replica_replication_port)

        # Create DB instance
        replica_server_args[BUILD_TYPE_KEY] = build_type
        self.replica = NoisePageServer(build_type=build_type,
                                       port=replica_server_args[SERVER_ARGS_KEY][PORT_KEY],
                                       server_args=replica_server_args[SERVER_ARGS_KEY])

    def setup(self):
        """
        Start the replica node
        """
        # Start DB
        if not self.replica.run_db(timeout=30):
            raise RuntimeError("Unable to start database")


    def run(self):
        """
        If we are testing throughput on the replica node then we start shipping logs to the replica. Otherwise we do
        nothing
        """
        if self.ship_logs:
            self.log_shipper.ship()

    def teardown(self):
        """
        Shut down the log shipper if needed and stop the replica node and delete it's WAL
        """
        if self.ship_logs:
            self.log_shipper.cleanup_zmq()
        self.replica.stop_db()
        self.replica.delete_wal()
