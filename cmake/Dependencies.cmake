# This list is required for static linking and exported to TERRIERConfig.cmake
set(TERRIER_LINKER_LIBS "")

# ---[ Jemalloc
find_package(JeMalloc)
include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})

# ---[ Google-gflags
find_package(GFlags)
include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
list(APPEND TERRIER_LINKER_LIBS ${GFLAGS_LIBRARIES})

# ---[ Google Test
set(gtest_SOURCE_DIR third_party/googletest)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

# ---[ Intel TBB
find_package(TBB REQUIRED)
include_directories(SYSTEM ${TBB_INCLUDE_DIRS})
list(APPEND TERRIER_LINKER_LIBS ${TBB_LIBRARIES})

# ---[  Google Test
list(APPEND TERRIER_LINKER_LIBS gtest_main)

# ---[ Doxygen
if(BUILD_docs)
    find_package(Doxygen)
endif()