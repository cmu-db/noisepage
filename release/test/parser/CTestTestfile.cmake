# CMake generated Testfile for 
# Source directory: /Users/vivianhuang/desktop/terrier/test/parser
# Build directory: /Users/vivianhuang/desktop/terrier/release/test/parser
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(expression_test "/Users/vivianhuang/desktop/terrier/build-support/run-test.sh" "/Users/vivianhuang/desktop/terrier/release" "test" "/Users/vivianhuang/desktop/terrier/release/release//expression_test")
set_tests_properties(expression_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;317;add_test;/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;327;ADD_TERRIER_TEST;/Users/vivianhuang/desktop/terrier/test/parser/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/vivianhuang/desktop/terrier/test/parser/CMakeLists.txt;0;")
add_test(parser_test "/Users/vivianhuang/desktop/terrier/build-support/run-test.sh" "/Users/vivianhuang/desktop/terrier/release" "test" "/Users/vivianhuang/desktop/terrier/release/release//parser_test")
set_tests_properties(parser_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;317;add_test;/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;327;ADD_TERRIER_TEST;/Users/vivianhuang/desktop/terrier/test/parser/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/vivianhuang/desktop/terrier/test/parser/CMakeLists.txt;0;")
add_test(pg_parser_test "/Users/vivianhuang/desktop/terrier/build-support/run-test.sh" "/Users/vivianhuang/desktop/terrier/release" "test" "/Users/vivianhuang/desktop/terrier/release/release//pg_parser_test")
set_tests_properties(pg_parser_test PROPERTIES  LABELS "unittest" _BACKTRACE_TRIPLES "/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;317;add_test;/Users/vivianhuang/desktop/terrier/cmake_modules/BuildUtils.cmake;327;ADD_TERRIER_TEST;/Users/vivianhuang/desktop/terrier/test/parser/CMakeLists.txt;1;ADD_TERRIER_TESTS;/Users/vivianhuang/desktop/terrier/test/parser/CMakeLists.txt;0;")
