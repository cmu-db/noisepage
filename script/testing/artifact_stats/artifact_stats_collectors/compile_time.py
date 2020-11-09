import os
import time
import subprocess

from artifact_stats.base_artifact_stats_collector import BaseArtifactStatsCollector
from artifact_stats.common_collector_functions import compile_binary


class CompileTimeCollector(BaseArtifactStatsCollector):

    def run_collector(self):
        """ Start a timer and run the compile commands """
        start_time = time.perf_counter()
        exit_code = compile_binary(self.build_path, use_cache=False, is_debug=self.is_debug)
        end_time = time.perf_counter()
        self.metrics['compile_time_sec'] = end_time - start_time
        return exit_code
