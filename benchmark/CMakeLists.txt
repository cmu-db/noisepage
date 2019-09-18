file(GLOB_RECURSE BENCHMARK_UTIL_SRCS ${PROJECT_SOURCE_DIR}/benchmark/util/*.cpp ${PROJECT_SOURCE_DIR}/benchmark/include/*.h)

###############################################
# Benchmark library
###############################################
if (TERRIER_BUILD_BENCHMARKS)
    add_library(benchmark_util STATIC ${BENCHMARK_UTIL_SRCS})
    target_link_libraries(benchmark_util test_util ${TERRIER_LINK_LIBS})
    add_dependencies(benchmark_util test_util benchmark)

    # benchmarks

    add_subdirectory(catalog)
    add_subdirectory(integration)
    add_subdirectory(storage)
    add_subdirectory(transaction)
endif ()
