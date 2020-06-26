# CMake generated Testfile for 
# Source directory: /Users/jeffniu/Desktop/terrier/benchmark/metrics
# Build directory: /Users/jeffniu/Desktop/terrier/release/benchmark/metrics
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(garbage_collection_metrics_benchmark "/Users/jeffniu/Desktop/terrier/build-support/run-test.sh" "/Users/jeffniu/Desktop/terrier/release" "benchmark" "/Users/jeffniu/Desktop/terrier/release/release//garbage_collection_metrics_benchmark" "--color_print=false")
set_tests_properties(garbage_collection_metrics_benchmark PROPERTIES  LABELS "benchmark" _BACKTRACE_TRIPLES "/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;240;add_test;/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;341;ADD_TERRIER_BENCHMARK;/Users/jeffniu/Desktop/terrier/benchmark/metrics/CMakeLists.txt;1;ADD_TERRIER_BENCHMARKS;/Users/jeffniu/Desktop/terrier/benchmark/metrics/CMakeLists.txt;0;")
add_test(large_transaction_metrics_benchmark "/Users/jeffniu/Desktop/terrier/build-support/run-test.sh" "/Users/jeffniu/Desktop/terrier/release" "benchmark" "/Users/jeffniu/Desktop/terrier/release/release//large_transaction_metrics_benchmark" "--color_print=false")
set_tests_properties(large_transaction_metrics_benchmark PROPERTIES  LABELS "benchmark" _BACKTRACE_TRIPLES "/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;240;add_test;/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;341;ADD_TERRIER_BENCHMARK;/Users/jeffniu/Desktop/terrier/benchmark/metrics/CMakeLists.txt;1;ADD_TERRIER_BENCHMARKS;/Users/jeffniu/Desktop/terrier/benchmark/metrics/CMakeLists.txt;0;")
add_test(logging_metrics_benchmark "/Users/jeffniu/Desktop/terrier/build-support/run-test.sh" "/Users/jeffniu/Desktop/terrier/release" "benchmark" "/Users/jeffniu/Desktop/terrier/release/release//logging_metrics_benchmark" "--color_print=false")
set_tests_properties(logging_metrics_benchmark PROPERTIES  LABELS "benchmark" _BACKTRACE_TRIPLES "/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;240;add_test;/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;341;ADD_TERRIER_BENCHMARK;/Users/jeffniu/Desktop/terrier/benchmark/metrics/CMakeLists.txt;1;ADD_TERRIER_BENCHMARKS;/Users/jeffniu/Desktop/terrier/benchmark/metrics/CMakeLists.txt;0;")
