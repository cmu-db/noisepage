import os
import time
import subprocess

from binary_metrics.base_binary_metrics_collector import BaseBinaryMetricsCollector
from util.constants import DIR_REPO, LOG


class CompileTimeCollector(BaseBinaryMetricsCollector):
    def __init__(self, isDebug):
        super().__init__(isDebug)
        cmake_cmd = 'cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_ASAN=OFF -DNOISEPAGE_USE_JEMALLOC=ON ..'
        ninja_cmd = 'ninja noisepage' #TODO: Should we control how ninja does parallelization to get more consistant numbers

        self._compile_commands = [cmake_cmd, ninja_cmd]

    def run_collector(self):
        """ Start a timer and run the compile commands """
        start_time = time.perf_counter()
        try:
            for cmd in self._compile_commands:
                subprocess.run(cmd, cwd=self.build_path, shell=True, check=True, capture_output=(not self.isDebug))
        except subprocess.CalledProcessError as err:
            LOG.debug(err.stdout)
            LOG.error(err.stderr)
            return err.returncode

        end_time = time.perf_counter()
        self.metrics['compile_time_sec'] = end_time - start_time
        return 0