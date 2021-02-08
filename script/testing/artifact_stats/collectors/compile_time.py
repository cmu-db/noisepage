import os

from ..base_artifact_stats_collector import BaseArtifactStatsCollector


class CompileTimeCollector(BaseArtifactStatsCollector):
    """
    Collect the compilation time required for NoisePage.

    Notes
    -----
    This doesn't actually compile NoisePage at all. It just reads a file
    which is assumed to exist at /tmp/noisepage-compiletime.txt.
    """

    compile_time_file_path = '/tmp/noisepage-compiletime.txt'

    def run_collector(self):
        """
        Read the file with compilation times, which is assumed to exist.

        Raises
        -------
        FileNotFoundError
            If no file exists at `self.compile_time_file_path`.
        """
        if not os.path.exists(self.compile_time_file_path):
            raise FileNotFoundError(f'{self.compile_time_file_path} not found.')
        with open(self.compile_time_file_path, 'r') as compile_time_file:
            self.metrics['compile_time_sec'] = float(compile_time_file.read())
        return 0
