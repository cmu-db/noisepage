file(GLOB_RECURSE TEST_UTIL_HDRS ${PROJECT_SOURCE_DIR}/test/include/*.h)
file(GLOB_RECURSE TEST_UTIL_SRCS ${PROJECT_SOURCE_DIR}/test/util/*.cpp)
list(APPEND TEST_UTIL_SRCS ${TEST_UTIL_HDRS})

###############################################
# Test library
###############################################
if (TERRIER_BUILD_TESTS)
    add_library(test_util STATIC ${TEST_UTIL_SRCS})
    target_link_libraries(test_util ${TERRIER_LINK_LIBS})
    add_dependencies(test_util gtest)

    add_subdirectory(common)
    add_subdirectory(execution)

    add_subdirectory(network)
    add_subdirectory(parser)
    add_subdirectory(storage)
    add_subdirectory(transaction)
    add_subdirectory(type)
    add_subdirectory(catalog)
    add_subdirectory(settings)
    add_subdirectory(planner)
    add_subdirectory(optimizer)
    add_subdirectory(traffic_cop)
    add_subdirectory(integration)
endif ()

