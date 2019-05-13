file(GLOB_RECURSE BENCHMARK_UTIL_HDRS ${PROJECT_SOURCE_DIR}/benchmark/include/*.h)
file(GLOB_RECURSE BENCHMARK_UTIL_SRCS ${PROJECT_SOURCE_DIR}/benchmark/util/*.cpp)
list(APPEND BENCHMARK_UTIL_SRCS ${BENCHMARK_UTIL_HDRS})

###############################################
# Benchmark library
###############################################
if (TERRIER_BUILD_BENCHMARKS)
    add_library(benchmark_util STATIC ${BENCHMARK_UTIL_SRCS})
    target_link_libraries(benchmark_util benchmark pthread)
    add_dependencies(benchmark_util gtest)

    add_subdirectory(catalog)
    add_subdirectory(storage)
    add_subdirectory(transaction)
endif()
