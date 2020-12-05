import os

from artifact_stats.base_artifact_stats_collector import BaseArtifactStatsCollector
from util.db_server import get_build_path

class BinarySizeCollector(BaseArtifactStatsCollector):
    
    def run_collector(self):
        """ Measure the size of the DBMS binary """        
        binary_path = get_build_path(build_type='release')
        self.metrics['binary_size'] = os.path.getsize(binary_path)
        return 0
