from abc import abstractmethod, ABC

from .constants import DEFAULT_PRIMARY_SERVER_ARGS, BUILD_TYPE_KEY, DEFAULT_OLTP_TEST_CASE, BENCHMARK_KEY, \
    SERVER_ARGS_KEY, REPLICATION_ENABLED_KEY, MESSENGER_ENABLED_KEY, DEFAULT_REPLICA_SERVER_ARGS, PORT_KEY, \
    METRICS_KEY, USE_METRICS_THREAD, LOGGING_METRICS_ENABLED_KEY
from .log_shipper import LogShipper
from .test_type import TestType
from ...oltpbench.test_case_oltp import TestCaseOLTPBench
from ...oltpbench.test_oltpbench import TestOLTPBench
from ...util.db_server import NoisePageServer


class NodeServer(ABC):

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def teardown(self):
        pass


class PrimaryNode(NodeServer):
    def __init__(self, build_type: str, replication_enabled: bool, oltp_benchmark: str):
        # Create DB instance
        primary_server_args = DEFAULT_PRIMARY_SERVER_ARGS
        primary_server_args[BUILD_TYPE_KEY] = build_type
        if replication_enabled:
            primary_server_args[SERVER_ARGS_KEY][MESSENGER_ENABLED_KEY] = True
            primary_server_args[SERVER_ARGS_KEY][REPLICATION_ENABLED_KEY] = True
        self.oltp_server = TestOLTPBench(primary_server_args)

        # Create OLTP test case
        oltp_test_case = DEFAULT_OLTP_TEST_CASE
        oltp_test_case[BENCHMARK_KEY] = oltp_benchmark
        self.test_case = TestCaseOLTPBench(oltp_test_case)

    def setup(self):
        # Start DB
        self.oltp_server.db_instance.run_db()
        # Download and prepare OLTP Bench
        self.oltp_server.run_pre_suite()

    def run(self):
        # Load DB
        self.test_case.run_pre_test()

    def teardown(self):
        self.oltp_server.db_instance.stop_db()
        self.oltp_server.db_instance.delete_wal()


class ReplicaNode(NodeServer):
    def __init__(self, test_type: TestType, build_type: str):
        replica_server_args = DEFAULT_REPLICA_SERVER_ARGS

        self.ship_logs = False
        # TODO
        if test_type == TestType.REPLICA:
            replica_server_args[SERVER_ARGS_KEY][METRICS_KEY] = True
            replica_server_args[SERVER_ARGS_KEY][USE_METRICS_THREAD] = True
            replica_server_args[SERVER_ARGS_KEY][LOGGING_METRICS_ENABLED_KEY] = True
            self.ship_logs = True
            self.log_shipper = LogShipper("")

        # Create DB instance
        replica_server_args[BUILD_TYPE_KEY] = build_type
        self.replica = NoisePageServer(build_type=build_type,
                                       port=replica_server_args[SERVER_ARGS_KEY][PORT_KEY],
                                       server_args=replica_server_args[SERVER_ARGS_KEY])

    def setup(self):
        # Start DB
        self.replica.run_db()

    def run(self):
        if self.ship_logs:
            # TODO
            self.log_shipper.ship()

    def teardown(self):
        self.replica.stop_db()
        self.replica.delete_wal()
