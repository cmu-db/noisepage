# Microbenchmark Script

This script executes the system's benchmarks and dumps out the results in JSON.
It also stores the results in Jenkins as an artifact.

If you add your benchmark to the list inside of this script, then it will run automatically in our 
nightly performance runs.

The script checks whether the performance of the benchmark has decreased by a certain amount 
compared to the average performance from the last 30 days.

## Requirements

This script assumes that you have numactl package installed.

```
sudo apt install numactl
```
