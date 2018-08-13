file(GLOB_RECURSE TEST_HDRS ${PROJECT_SOURCE_DIR}/test/include/*.h)
file(GLOB_RECURSE TEST_UTIL_SRCS ${PROJECT_SOURCE_DIR}/test/util/*.cpp)
list(APPEND TEST_UTIL_SRCS ${TEST_HDRS})

###############################################
# Test library
###############################################
add_library(test_shared SHARED ${TEST_UTIL_SRCS})
target_link_libraries(test_shared ${TERRIER_LINK_LIBS})

add_subdirectory(common)
add_subdirectory(storage)
add_subdirectory(transaction)
