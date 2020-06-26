# CMake generated Testfile for 
# Source directory: /Users/jeffniu/Desktop/terrier/test/network
# Build directory: /Users/jeffniu/Desktop/terrier/release/test/network
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(network_test "/Users/jeffniu/Desktop/terrier/build-support/run-test.sh" "/Users/jeffniu/Desktop/terrier/release" "test" "/Users/jeffniu/Desktop/terrier/release/release//network_test")
set_tests_properties(network_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;323;add_test;/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;333;ADD_TERRIER_TEST;/Users/jeffniu/Desktop/terrier/test/network/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/jeffniu/Desktop/terrier/test/network/CMakeLists.txt;0;")
add_test(postgres_protocol_util_test "/Users/jeffniu/Desktop/terrier/build-support/run-test.sh" "/Users/jeffniu/Desktop/terrier/release" "test" "/Users/jeffniu/Desktop/terrier/release/release//postgres_protocol_util_test")
set_tests_properties(postgres_protocol_util_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;323;add_test;/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;333;ADD_TERRIER_TEST;/Users/jeffniu/Desktop/terrier/test/network/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/jeffniu/Desktop/terrier/test/network/CMakeLists.txt;0;")
