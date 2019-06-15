# CMake generated Testfile for 
# Source directory: /Users/vivianhuang/desktop/terrier/test/integration
# Build directory: /Users/vivianhuang/desktop/terrier/release/test/integration
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(tpcc_test "/Users/vivianhuang/desktop/terrier/build-support/run-test.sh" "/Users/vivianhuang/desktop/terrier/release" "test" "/Users/vivianhuang/desktop/terrier/release/release//tpcc_test")
set_tests_properties(tpcc_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;317;add_test;/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;327;ADD_TERRIER_TEST;/Users/vivianhuang/desktop/terrier/test/integration/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/vivianhuang/desktop/terrier/test/integration/CMakeLists.txt;0;")
