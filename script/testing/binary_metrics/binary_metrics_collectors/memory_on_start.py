import os
import time
import subprocess
import psutil

from binary_metrics.base_binary_metrics_collector import BaseBinaryMetricsCollector
from util.test_server import kill_processes_listening_on_db_port, start_db, check_db_running
from util.constants import DEFAULT_DB_HOST, DEFAULT_DB_PORT, DEFAULT_DB_OUTPUT_FILE, LOG

class MemoryOnStartCollector(BaseBinaryMetricsCollector):
    def __init__(self, isDebug):
        super().__init__(isDebug)

    def setup(self):
        super().setup()
        compile_binaries(self.isDebug)

        # TODO: fix the stuff below because things changed
        kill_processes_listening_on_db_port(DEFAULT_DB_PORT)
        db_output_fd, db_process = start_db(os.path.join(self.build_path,'release','terrier'), DEFAULT_DB_OUTPUT_FILE)
        self.db_process = db_process
        if not db_has_started():
            raise Error('DB never started')


    def run_collector(self):
        process = psutil.Process(self.db_process.pid)
        self.metrics['db_rss_on_start'] = process.memory_info().rss

    def teardown(self):
        self.db_process.terminate()
        super().teardown()

def compile_binaries(isDebug):        
    cmake_cmd = 'cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_ASAN=OFF -DNOISEPAGE_USE_JEMALLOC=ON ..'
    ninja_cmd = 'ninja noisepage'
    compile_commands = [cmake_cmd,ninja_cmd]

    try:
        for cmd in compile_commands:
            subprocess.run(cmd, cwd='build', shell=True, check=True, capture_output=(not isDebug))
    except subprocess.CalledProcessError as err:
        LOG.debug(err.stdout)
        LOG.error(err.stderr)

def db_has_started():
    exponential_backoff = 0
    while not check_db_running(DEFAULT_DB_HOST, DEFAULT_DB_PORT) and exponential_backoff <= 10:
        time.sleep(exponential_backoff)
        exponential_backoff += 1
    return exponential_backoff <= 10 