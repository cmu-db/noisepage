#!/bin/sh

cd ../build
rm daf_count_agg_woLogging_1thread.csv daf_time_agg_woLogging_1thread.csv daf_count_agg_woLogging_4thread.csv daf_time_agg_woLogging_4thread.csv daf_count_agg_woLogging_8thread.csv daf_time_agg_woLogging_8thread.csv 2> /dev/null
rm num_txn_processed_1thread.csv num_txn_processed_4thread.csv num_txn_processed_8thread.csv  2> /dev/null
cmake -DTERRIER_USE_ASAN=OFF -DCMAKE_BUILD_TYPE=Release -DTERRIER_USE_JEMALLOC=ON .. && make tpcc_benchmark -j12
export TERRIER_BENCHMARK_THREADS=1
./release/tpcc_benchmark 
mv daf_count_agg.csv daf_count_agg_woLogging_1thread.csv
mv daf_time_agg.csv daf_time_agg_woLogging_1thread.csv
# mv num_txn_processed.csv num_txn_processed_1thread.csv
export TERRIER_BENCHMARK_THREADS=4
./release/tpcc_benchmark 
mv daf_count_agg.csv daf_count_agg_woLogging_4thread.csv
mv daf_time_agg.csv daf_time_agg_woLogging_4thread.csv
# mv num_txn_processed.csv num_txn_processed_4thread.csv
# sudo su
export TERRIER_BENCHMARK_THREADS=8
./release/tpcc_benchmark 
# perf timechart ./release/tpcc_benchmark
# perf report
mv daf_count_agg.csv daf_count_agg_woLogging_8thread.csv
mv daf_time_agg.csv daf_time_agg_woLogging_8thread.csv
# mv num_txn_processed.csv num_txn_processed_8thread.csv
cd ..