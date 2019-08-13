file(GLOB_RECURSE TEST_UTIL_SRCS ${PROJECT_SOURCE_DIR}/test/util/*.cpp ${PROJECT_SOURCE_DIR}/test/include/*.h)

###############################################
# Test library
###############################################
if (TERRIER_BUILD_TESTS)
    add_library(test_util STATIC ${TEST_UTIL_SRCS})
    target_link_libraries(test_util terrier_static util_static ${TERRIER_LINK_LIBS})
    add_dependencies(test_util terrier_static util_static gtest gmock_main)

    # tests

    add_subdirectory(catalog)
    add_subdirectory(common)
    add_subdirectory(execution)
    add_subdirectory(integration)
    add_subdirectory(network)
    add_subdirectory(optimizer)
    add_subdirectory(parser)
    add_subdirectory(planner)
    add_subdirectory(settings)
    add_subdirectory(storage)
    add_subdirectory(traffic_cop)
    add_subdirectory(transaction)
    add_subdirectory(type)
endif ()

