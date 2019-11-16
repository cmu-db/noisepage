# CMake generated Testfile for 
# Source directory: /Users/dpatra/Research/terrier/benchmark/transaction
# Build directory: /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/benchmark/transaction
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(large_transaction_benchmark "/Users/dpatra/Research/terrier/build-support/run-test.sh" "/Users/dpatra/Research/terrier/cmake-build-relwithdebinfo" "benchmark" "/Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/relwithdebinfo//large_transaction_benchmark" "--color_print=false")
set_tests_properties(large_transaction_benchmark PROPERTIES  LABELS "benchmark" _BACKTRACE_TRIPLES "/Users/dpatra/Research/terrier/cmake_modules/BuildUtils.cmake;239;add_test;/Users/dpatra/Research/terrier/cmake_modules/BuildUtils.cmake;335;ADD_TERRIER_BENCHMARK;/Users/dpatra/Research/terrier/benchmark/transaction/CMakeLists.txt;1;ADD_TERRIER_BENCHMARKS;/Users/dpatra/Research/terrier/benchmark/transaction/CMakeLists.txt;0;")
add_test(seqscan_benchmark "/Users/dpatra/Research/terrier/build-support/run-test.sh" "/Users/dpatra/Research/terrier/cmake-build-relwithdebinfo" "benchmark" "/Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/relwithdebinfo//seqscan_benchmark" "--color_print=false")
set_tests_properties(seqscan_benchmark PROPERTIES  LABELS "benchmark" _BACKTRACE_TRIPLES "/Users/dpatra/Research/terrier/cmake_modules/BuildUtils.cmake;239;add_test;/Users/dpatra/Research/terrier/cmake_modules/BuildUtils.cmake;335;ADD_TERRIER_BENCHMARK;/Users/dpatra/Research/terrier/benchmark/transaction/CMakeLists.txt;1;ADD_TERRIER_BENCHMARKS;/Users/dpatra/Research/terrier/benchmark/transaction/CMakeLists.txt;0;")
