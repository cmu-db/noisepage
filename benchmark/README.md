# Benchmarks Directory

These are the microbenchmarks for the DBMS. We use [Google Benchmark](https://github.com/google/benchmark) library for all of these.

**IMPORTANT:** If you add a new microbenchmark, you must add it to `run_micro_bench.py` to have it run automatically in our nightly builds.

## Usage

All of the multi-threaded benchmarks use one thread by default:

```
$ ./data_table_benchmark
```

You can set the number of threads to use with the `NOISEPAGE_BENCHMARK_THREADS` environment variable:

```
$ NOISEPAGE_BENCHMARK_THREADS="9" ./data_table_benchmark
```
