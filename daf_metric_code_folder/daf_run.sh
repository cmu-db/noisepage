#!/bin/sh

rm -r ../daf_metric_results_folder/*
cd ../daf/terrier/build
rm *.csv agg_expr_results.txt *result.json 2> /dev/null
NOW=$(date +%m%d%Y)
cmake -DTERRIER_USE_ASAN=OFF -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_JEMALLOC=ON .. && make tpcc_benchmark -j30
for num_daf_threads in 1 2 4 8
do
	export TERRIER_BENCHMARK_DAF_THREADS=$num_daf_threads
	for num_worker_threads in 1 4 8 12 16 20
	do
		export TERRIER_BENCHMARK_THREADS=$num_worker_threads
		./release/tpcc_benchmark --benchmark_out_format=json --benchmark_out=${num_daf_threads}daf_${num_worker_threads}worker_result.json
		mv daf_count_agg.csv ${num_daf_threads}daf_count_agg_woLogging_${num_worker_threads}thread.csv
		mv daf_time_agg.csv ${num_daf_threads}daf_time_agg_woLogging_${num_worker_threads}thread.csv
		cat expr_results.csv >> agg_expr_results.txt
	done
	mkdir ${num_daf_threads}DAF_TPCC_${NOW}
	mv *.csv *result.json ${num_daf_threads}DAF_TPCC_${NOW}
	mv ${num_daf_threads}DAF_TPCC_${NOW} ../../../daf_metric_results_folder/
done
mv agg_expr_results.txt agg_expr_results.csv
mv agg_expr_results.csv ../../../daf_metric_results_folder/
