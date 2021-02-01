# Microbenchmark Script

This script executes the system's benchmarks and dumps out the results in JSON and XML.

If you add your benchmark to `micro_bench/benchmarks.py`, then it will run automatically in our 
nightly performance runs.

The script checks whether the performance of the benchmark has decreased by a certain amount 
compared to the average performance from the last 30 days.

The script will send the results to incrudibles-production.db.pdl.cmu.edu where they will be stored
in TimescaleDB. The results will be visualized at [stats.noise.page](https://stats.noise.page).

## Requirements

This script assumes that you have numactl package installed. If you are running the script locally you do not need to
 install `numactl` but you will need to specify `--local`. 

```
sudo apt install numactl
```


## Usage

By default, the script will run all of the microbenchmarks and compare against the results stored 
in the Jenkins repository. It assumes that the microbenchmark binaries can be found in the default 
benchmark path.

```
$ ./run_micro_bench.py
```

If you only want to run a subset microbenchmark, you can pass in the **name** of the microbenchmark 
binary (e.g., `data_table_benchmark`) and not the suite name (e.g., `DataTableBenchmark`):

```
$ ./run_micro_bench.py data_table_benchmark recovery_benchmark
```

## Local Execution

Comparing against the Jenkins results repository is not useful if you are running on your laptop 
because the hardware will be different. If you want to run the benchmark multiple times locally and 
perform the same performance threshold checks that we do in Jenkins, you can pass in the `--local` 
flag. That will write the results to a directory in the same path as the script. It can then 
compute the average results for the microbenchmarks for all the local runs:

```
$ ./run_micro_bench.py --local
```

This will write the results of each invocation to a directory called "local". See note in the requirements section about `numactl`.

## Perf Profiling

You can pass the `--perf` flag to enable the script to run the microbenchmarks with perf enabled. 
This makes it easier to do profiling from the script without needing to track down the environment 
variables.

```
$ ./run_micro_bench.py --perf data_table_benchmark
```

The script will configure perf to write its trace file to `data_table_benchmark.perf`.

## Publish Results

By specifying the `--publish-results` the microbenchmark run script will send the results to the performance storage
service. This will also require passing in the `--publish-username` and `--publish-password` arguments. 
