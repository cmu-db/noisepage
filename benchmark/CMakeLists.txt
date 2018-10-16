file(GLOB_RECURSE BENCHMARK_UTIL_HDRS ${PROJECT_SOURCE_DIR}/benchmark/include/*.h)
file(GLOB_RECURSE BENCHMARK_UTIL_SRCS ${PROJECT_SOURCE_DIR}/benchmark/util/*.cpp)
list(APPEND BENCHMARK_UTIL_SRCS ${BENCHMARK_UTIL_HDRS})

###############################################
# Benchmark library
###############################################
add_library(benchmark_util STATIC ${BENCHMARK_UTIL_SRCS})
target_link_libraries(benchmark_util benchmark)
add_dependencies(benchmark_util gtest)

add_subdirectory(storage)
add_subdirectory(transaction)
