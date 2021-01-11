import psutil

from ...util.db_server import NoisePageServer
from ..base_artifact_stats_collector import BaseArtifactStatsCollector


class MemoryOnStartCollector(BaseArtifactStatsCollector):
    """
    Measure the memory consumption of the NoisePage DBMS in release mode
    on startup.

    Notes
    -----
    The NoisePage DBMS is assumed to have been started by the same user as the
    user running this script, otherwise sudo permission may be required.

    It is assumed that no other instance of the NoisePage server is running.
    """

    def setup(self):
        super().setup()
        self.db_instance = NoisePageServer(build_type='release')
        self.db_instance.run_db()

    def run_collector(self):
        """
        Measure the memory consumption of the DBMS process.
        """
        process = psutil.Process(self.db_instance.db_process.pid)
        memory_data = process.memory_info()
        self.metrics['rss_on_start'] = memory_data.rss
        self.metrics['vms_on_start'] = memory_data.vms

    def teardown(self):
        super().teardown()
        self.db_instance.stop_db()
