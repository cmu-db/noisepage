import os

from artifact_stats.base_artifact_stats_collector import BaseArtifactStatsCollector


class CompileTimeCollector(BaseArtifactStatsCollector):
    compile_time_file_path = '/tmp/compiletime.txt'

    def run_collector(self):
        """ Start a timer and run the compile commands """
        if not os.path.exists(self.compile_time_file_path):
            raise FileNotFoundError(f'{self.compile_time_file_path} not found.')
        with open(self.compile_time_file_path, 'r') as compile_time_file:
            self.metrics['compile_time_sec'] = float(compile_time_file.read())
        return 0
