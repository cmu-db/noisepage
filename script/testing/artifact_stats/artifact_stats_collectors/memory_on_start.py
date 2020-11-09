import os
import time
import subprocess
import psutil

from artifact_stats.base_artifact_stats_collector import BaseArtifactStatsCollector
from util.db_server import NoisePageServer
from artifact_stats.common_collector_functions import compile_binary


class MemoryOnStartCollector(BaseArtifactStatsCollector):
    def __init__(self, is_debug):
        super().__init__(is_debug)

    def setup(self):
        super().setup()
        compile_binary(self.build_path, is_debug=self.is_debug)
        self.db_instance = NoisePageServer(build_type='release')
        self.db_instance.run_db()

    def run_collector(self):
        process = psutil.Process(self.db_instance.db_process.pid)
        memory_data = process.memory_info()
        self.metrics['rss_on_start'] = memory_data.rss
        self.metrics['vms_on_start'] = memory_data.vms

    def teardown(self):
        super().teardown()
        self.db_instance.stop_db()
