# Artifact Stats
These metrics are non-operational measurements of the DBMS.

## Current Metrics
- Binary size
- Compile time
- Memory usage on startup
- (coming soon) Idle CPU utilization

## How to add a metric
1) Create a file for your collector in `/script/testing/artifact_stats/collectors`.
2) Create a sub class of the `BaseArtifactStatsCollector` class.
    - See `collectors/compile_time.py` for a simple example.
    - See `collectors/memory_on_start.py` for an example that requires running the DBMS.
    - See `BaseArtifactStatsCollector` in `base_artifact_stats_collector.py` for more details.
3) Import the new collector in `/script/testing/artifact_stats/collectors/__init__.py`.
4) Test it out by running the `artifact_stats` module and see if your metric has been added to the artifact stats.

## Script

From `script`, run `python3 -m testing.artifact_stats`.

### Args
- `--debug` - Run with debug logging.

- `--publish-results` - The environment to publish the results to (test, staging, or prod). If omitted the script will not publish the results.

- `--publish-username` - The username used to authenticate with the performance storage service.

- `--publish-password` - The password used to authenticate with the performance storage service.

## Other notes

- Read the class docstring for `BaseArtifactStatsCollector`.
- All collectors appear to be written assuming exclusive use of the system. If you try to run collectors in parallel or while other NoisePage instances are running, you're in for a bad time.
