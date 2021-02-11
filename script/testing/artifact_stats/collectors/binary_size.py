import os

from ...util.db_server import get_build_path
from ..base_artifact_stats_collector import BaseArtifactStatsCollector


class BinarySizeCollector(BaseArtifactStatsCollector):
    """
    Collect the size of the NoisePage DBMS release binary.

    Notes
    -----
    This assumes that the release binary is located in a folder called either
    `build` or `cmake-build-release`.
    """

    def run_collector(self):
        """
        Measure the size of the NoisePage DBMS release binary.
        """
        binary_path = get_build_path(build_type='release')
        self.metrics['binary_size'] = os.path.getsize(binary_path)
        return 0
