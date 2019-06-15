# CMake generated Testfile for 
# Source directory: /Users/vivianhuang/desktop/terrier/benchmark/integration
# Build directory: /Users/vivianhuang/desktop/terrier/release/benchmark/integration
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(tpcc_benchmark "/Users/vivianhuang/desktop/terrier/build-support/run-test.sh" "/Users/vivianhuang/desktop/terrier/release" "benchmark" "/Users/vivianhuang/desktop/terrier/release/release//tpcc_benchmark" "--color_print=false")
set_tests_properties(tpcc_benchmark PROPERTIES  LABELS "benchmark" _BACKTRACE_TRIPLES "/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;239;add_test;/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;335;ADD_TERRIER_BENCHMARK;/Users/vivianhuang/desktop/terrier/benchmark/integration/CMakeLists.txt;1;ADD_TERRIER_BENCHMARKS;/Users/vivianhuang/desktop/terrier/benchmark/integration/CMakeLists.txt;0;")
