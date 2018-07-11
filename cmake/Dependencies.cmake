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

# --[ LLVM 6.0+
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "6.0")
    message( FATAL_ERROR "LLVM 6.0 or newer is required." )
endif()
llvm_map_components_to_libnames(LLVM_LIBRARIES core mcjit nativecodegen native)
include_directories(SYSTEM ${LLVM_INCLUDE_DIRS})
list(APPEND TERRIER_LINKER_LIBS ${LLVM_LIBRARIES})

# ---[ Doxygen
if(BUILD_docs)
    find_package(Doxygen)
endif()