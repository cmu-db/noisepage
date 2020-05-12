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

## Usage

By default, the script will run all of the microbenchmarks and compare against the results stored 
in the Jenkins repository. It assumes that the microbenchmark binaries can be found in the default 
benchmark path.

```
$ ./run_micro_bench.py --run
```

If you only want to run a subset microbenchmark, you can pass in the **name** of the microbenchmark 
binary (e.g., `data_table_benchmark`) and not the suite name (e.g., `DataTableBenchmark`):

```
$ ./run_micro_bench.py --run data_table_benchmark recovery_benchmark
```

Instead of printing out the human-readable table of results after running the microbenchmarks, you 
can also have it print out a CSV table:

```
$ ./run_micro_bench.py --run --csv-dump
```

## Local Execution

Comparing against the Jenkins results repository is not useful if you are running on your laptop 
because the hardware will be different. If you want to run the benchmark multiple times locally and 
perform the same performance threshold checks that we do in Jenkins, you can pass in the `--local` 
flag. That will write the results to a directory in the same path as the script. It can then 
compute the average results for the microbenchmarks for all the local runs:

```
$ ./run_micro_bench.py --run --local
```

This will write the results of each invocation to a directory called "local".

## Perf Profiling

You can pass the `--perf` flag to enable the script to run the microbenchmarks with perf enabled. 
This makes it easier to do profiling from the script without needing to track down the environment 
variables.

```
$ ./run_micro_bench.py --run --perf data_table_benchmark
```

The script will configure perf to write its trace file to `data_table_benchmark.perf`.
