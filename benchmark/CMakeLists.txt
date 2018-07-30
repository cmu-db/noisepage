file(GLOB_RECURSE BENCHMARK_HDRS ${PROJECT_SOURCE_DIR}/benchmark/include/*.h)
file(GLOB_RECURSE BENCHMARK_UTIL_SRCS ${PROJECT_SOURCE_DIR}/benchmark/util/*.cpp)
list(APPEND BENCHMARK_UTIL_SRCS ${BENCHMARK_HDRS})

###############################################
# Benchmark library
###############################################
add_library(benchmark_shared SHARED ${BENCHMARK_UTIL_SRCS})
target_link_libraries(benchmark_shared benchmark)

add_subdirectory(storage)
