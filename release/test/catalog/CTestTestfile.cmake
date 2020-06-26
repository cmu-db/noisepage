# CMake generated Testfile for 
# Source directory: /Users/jeffniu/Desktop/terrier/test/catalog
# Build directory: /Users/jeffniu/Desktop/terrier/release/test/catalog
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(catalog_test "/Users/jeffniu/Desktop/terrier/build-support/run-test.sh" "/Users/jeffniu/Desktop/terrier/release" "test" "/Users/jeffniu/Desktop/terrier/release/release//catalog_test")
set_tests_properties(catalog_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;323;add_test;/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;333;ADD_TERRIER_TEST;/Users/jeffniu/Desktop/terrier/test/catalog/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/jeffniu/Desktop/terrier/test/catalog/CMakeLists.txt;0;")
add_test(name_builder_test "/Users/jeffniu/Desktop/terrier/build-support/run-test.sh" "/Users/jeffniu/Desktop/terrier/release" "test" "/Users/jeffniu/Desktop/terrier/release/release//name_builder_test")
set_tests_properties(name_builder_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;323;add_test;/Users/jeffniu/Desktop/terrier/cmake_modules/BuildUtils.cmake;333;ADD_TERRIER_TEST;/Users/jeffniu/Desktop/terrier/test/catalog/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/jeffniu/Desktop/terrier/test/catalog/CMakeLists.txt;0;")
