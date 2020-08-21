#!/bin/sh

NOW=$(date +%m%d%Y)
rm -r ../coor_metric_results_folder_${NOW} 2> /dev/null
mkdir ../coor_metric_results_folder_${NOW}
cd ../coor/terrier/build
rm daf_count_agg.csv daf_time_agg.csv expr_results.csv
mv *.csv *result.json ../../../coor_metric_results_folder/
rm *.csv *result.json 2> /dev/null
cmake -DTERRIER_USE_ASAN=OFF -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_JEMALLOC=ON .. && make tpcc_benchmark -j30
# for num_worker_threads in 1 4 8 12 16 20 24 28 32 36 44
for num_worker_threads in 28 32 36 44
do
	export TERRIER_BENCHMARK_THREADS=$num_worker_threads
	./release/tpcc_benchmark --benchmark_repetitions=1 --benchmark_out_format=json --benchmark_out=coor_${num_worker_threads}worker_result.json
	mv daf_count_agg.csv coor_count_agg_woLogging_${num_worker_threads}thread.csv
	mv daf_time_agg.csv coor_time_agg_woLogging_${num_worker_threads}thread.csv
	cat expr_results.csv >> ../../../coor_metric_results_folder_${NOW}/agg_expr_results.csv
done
mv *.csv *result.json ../../../coor_metric_results_folder_${NOW}/
