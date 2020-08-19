#!/bin/sh

rm -r ../coor_metric_results_folder
mkdir ../coor_metric_results_folder
cd ../coor/terrier/build
rm *.csv *result.json 2> /dev/null
NOW=$(date +%m%d%Y)
cmake -DTERRIER_USE_ASAN=OFF -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_JEMALLOC=ON .. && make tpcc_benchmark -j30
for num_worker_threads in 1 4 8 12 16 20
do
	export TERRIER_BENCHMARK_THREADS=$num_worker_threads
	./release/tpcc_benchmark --benchmark_out_format=json --benchmark_out=coor_${num_worker_threads}worker_result.json
	mv daf_count_agg.csv coor_count_agg_woLogging_${num_worker_threads}thread.csv
	mv daf_time_agg.csv coor_time_agg_woLogging_${num_worker_threads}thread.csv
	cat expr_results.csv >> agg_expr_results.csv
done
mv *.csv *result.json ../../../coor_metric_results_folder/
