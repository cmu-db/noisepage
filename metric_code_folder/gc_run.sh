#!/bin/sh

rm -r ../gc_metric_results_folder
mkdir ../gc_metric_results_folder
cd ../master/terrier/build
rm *.csv *result.json 2> /dev/null
NOW=$(date +%m%d%Y)
cmake -DTERRIER_USE_ASAN=OFF -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_JEMALLOC=ON .. && make tpcc_benchmark -j30
for num_worker_threads in 1 4 8 12 16 20 24 28 32 36
do
	export TERRIER_BENCHMARK_THREADS=$num_worker_threads
	./release/tpcc_benchmark --benchmark_repetitions=5 --benchmark_out_format=json --benchmark_out=gc_${num_worker_threads}worker_result.json
	mv num_txn_processed.csv gc_num_txn_processed_woLogging_${num_worker_threads}thread.csv
	cat expr_results.csv >> ../../../gc_metric_results_folder/agg_expr_results.csv
	rm txn_begin.csv txn_commit.csv 
done
mv *.csv *result.json ../../../gc_metric_results_folder/
