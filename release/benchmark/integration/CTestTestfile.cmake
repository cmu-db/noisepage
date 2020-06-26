# CMake generated Testfile for 
# Source directory: /Users/jeffniu/Desktop/terrier/benchmark/integration
# Build directory: /Users/jeffniu/Desktop/terrier/release/benchmark/integration
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(tpcc_benchmark "/Users/jeffniu/Desktop/terrier/build-support/run-test.sh" "/Users/jeffniu/Desktop/terrier/release" "benchmark" "/Users/jeffniu/Desktop/terrier/release/release//tpcc_benchmark" "--color_print=false")
set_tests_properties(tpcc_benchmark PROPERTIES  LABELS "benchmark" _BACKTRACE_TRIPLES "/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;240;add_test;/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;341;ADD_TERRIER_BENCHMARK;/Users/jeffniu/Desktop/terrier/benchmark/integration/CMakeLists.txt;1;ADD_TERRIER_BENCHMARKS;/Users/jeffniu/Desktop/terrier/benchmark/integration/CMakeLists.txt;0;")
