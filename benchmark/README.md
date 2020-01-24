# Benchmarks Directory

These are the microbenchmarks for the DBMS. We use [Google Benchmark](https://github.com/google/benchmark) library for all of these.

See `run_micro_bench.py` on how these are executed in our nightly builds.

## Usage

All of the multi-threaded benchmarks use one thread by default:

```
$ ./data_table_benchmark
```

You can set the number of threads to use with the `TERRIER_BENCHMARK_THREADS` environment variable:

```
$ TERRIER_BENCHMARK_THREADS="9" ./data_table_benchmark
```
