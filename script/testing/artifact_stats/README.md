# Artifact Stats
These metrics are non-operational measurements of the DBMS.

## Current Metrics
- Compile time
- Memory usage on startup
- (coming soon) Idle CPU utilization
- (coming soon) binary size

## How to add a metric
1) Create a file for your collector in `/script/testing/artifact_stats/collectors` 
2) Create a sub class of the `BaseArtifactStatsCollector` class.
    - See `collectors/compile_time.py` for a simple example
    - See `collectors/memory_on_start.py` for an example that requires running the DBMS.
    - See [BaseArtifactStatsCollector](#BaseArtifactStatsCollector) for more details.
3) Import the new collector in `/script/testing/artifact_stats/collectors/__init__.py`
4) Test it out by running `/script/testing/artifact_stats/run_artifact_stats.py` and see if your metric has been added to the artifact stats.

## Script
`/script/testing/artifact_stats/run_artifact_stats.py`
### Args
`--debug` - Run with debug logging.

`--publish-results` - The environment to publish the results to (test, staging, or prod). If omitted the script will not publish the results.

`--publish-username` - The username used to authenticate with the performance storage service.

`--publish-password` - The password used to authenticate with the performance storage service.

## <a id="BaseArtifactStatsCollector"></a>BaseArtifactStatsCollector 
### Attributes
`is_debug` - This determines whether debug output from the collector will be printed in stdout. In many cases this will be the stdout of a spawned process.

`metrics` - The metrics collected during the execution of the collector.

### Methods
`__init__(is_debug)` - Initialize the class.

> args:
>> `is_debug`: sets the `is_debug` property for the collector.

`setup()` - This will run any steps that are needed to execute prior to the metric collection.

`run_collector()` - This will perform the setps needed to capture the artifact stats for the collector. This returns an exit code for the script. If everything succeeded return `0`.

`teardown()` - This will cleanup anything that was created or run during the `setup` or `run_collector` functions.

`get_metrics()` - This will return the metrics dict. This is used by the `run_artifact_stats.py` script to aggregate the metrics from each collector. In most cases this function should not be overwritten.

