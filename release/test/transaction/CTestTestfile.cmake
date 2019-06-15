# CMake generated Testfile for 
# Source directory: /Users/vivianhuang/desktop/terrier/test/transaction
# Build directory: /Users/vivianhuang/desktop/terrier/release/test/transaction
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(deferred_actions_test "/Users/vivianhuang/desktop/terrier/build-support/run-test.sh" "/Users/vivianhuang/desktop/terrier/release" "test" "/Users/vivianhuang/desktop/terrier/release/release//deferred_actions_test")
set_tests_properties(deferred_actions_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;317;add_test;/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;327;ADD_TERRIER_TEST;/Users/vivianhuang/desktop/terrier/test/transaction/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/vivianhuang/desktop/terrier/test/transaction/CMakeLists.txt;0;")
add_test(large_transaction_test "/Users/vivianhuang/desktop/terrier/build-support/run-test.sh" "/Users/vivianhuang/desktop/terrier/release" "test" "/Users/vivianhuang/desktop/terrier/release/release//large_transaction_test")
set_tests_properties(large_transaction_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;317;add_test;/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;327;ADD_TERRIER_TEST;/Users/vivianhuang/desktop/terrier/test/transaction/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/vivianhuang/desktop/terrier/test/transaction/CMakeLists.txt;0;")
add_test(mvcc_test "/Users/vivianhuang/desktop/terrier/build-support/run-test.sh" "/Users/vivianhuang/desktop/terrier/release" "test" "/Users/vivianhuang/desktop/terrier/release/release//mvcc_test")
set_tests_properties(mvcc_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;317;add_test;/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;327;ADD_TERRIER_TEST;/Users/vivianhuang/desktop/terrier/test/transaction/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/vivianhuang/desktop/terrier/test/transaction/CMakeLists.txt;0;")
