# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Modified from the Apache Arrow project for the Terrier project.

# ----------------------------------------------------------------------
# Thirdparty versions, environment variables, source URLs

set(THIRDPARTY_DIR "${CMAKE_SOURCE_DIR}/third_party")

if (NOT "$ENV{TERRIER_BUILD_TOOLCHAIN}" STREQUAL "")
    set(JEMALLOC_HOME "$ENV{TERRIER_BUILD_TOOLCHAIN}")
    set(GFLAGS_HOME "$ENV{TERRIER_BUILD_TOOLCHAIN}")
    set(LIBEVENT_HOME "$ENV{TERRIER_BUILD_TOOLCHAIN}")
endif ()

if (DEFINED ENV{GFLAGS_HOME})
    set(GFLAGS_HOME "$ENV{GFLAGS_HOME}")
endif ()

# ----------------------------------------------------------------------
# Versions and URLs for toolchain builds, which also can be used to configure
# offline builds

# Read toolchain versions from cpp/thirdparty/versions.txt
file(STRINGS "${THIRDPARTY_DIR}/versions.txt" TOOLCHAIN_VERSIONS_TXT)
foreach (_VERSION_ENTRY ${TOOLCHAIN_VERSIONS_TXT})
    # Exclude comments
    if (_VERSION_ENTRY MATCHES "#.*")
        continue()
    endif ()

    string(REGEX MATCH "^[^=]*" _LIB_NAME ${_VERSION_ENTRY})
    string(REPLACE "${_LIB_NAME}=" "" _LIB_VERSION ${_VERSION_ENTRY})

    # Skip blank or malformed lines
    if (${_LIB_VERSION} STREQUAL "")
        continue()
    endif ()

    # For debugging
    message(STATUS "${_LIB_NAME}: ${_LIB_VERSION}")

    set(${_LIB_NAME} "${_LIB_VERSION}")
endforeach ()

if (DEFINED ENV{TERRIER_GTEST_URL})
    set(GTEST_SOURCE_URL "$ENV{TERRIER_GTEST_URL}")
else ()
    set(GTEST_SOURCE_URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz")
endif ()

if (DEFINED ENV{TERRIER_GFLAGS_URL})
    set(GFLAGS_SOURCE_URL "$ENV{TERRIER_GFLAGS_URL}")
else ()
    set(GFLAGS_SOURCE_URL "https://github.com/gflags/gflags/archive/v${GFLAGS_VERSION}.tar.gz")
endif ()

if (DEFINED ENV{TERRIER_GBENCHMARK_URL})
    set(GBENCHMARK_SOURCE_URL "$ENV{TERRIER_GBENCHMARK_URL}")
else ()
    set(GBENCHMARK_SOURCE_URL "https://github.com/google/benchmark/archive/v${GBENCHMARK_VERSION}.tar.gz")
endif ()

# ----------------------------------------------------------------------
# ExternalProject options

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

if (NOT TERRIER_VERBOSE_THIRDPARTY_BUILD)
    set(EP_LOG_OPTIONS
            LOG_CONFIGURE 1
            LOG_BUILD 1
            LOG_INSTALL 1
            LOG_DOWNLOAD 1)
else ()
    set(EP_LOG_OPTIONS)
endif ()

# Set -fPIC on all external projects
set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
set(EP_C_FLAGS "${EP_C_FLAGS} -fPIC")

# Ensure that a default make is set
if ("${MAKE}" STREQUAL "")
    find_program(MAKE make)
endif ()

if (TERRIER_BUILD_TESTS OR TERRIER_BUILD_BENCHMARKS)
    add_custom_target(unittest ctest -L unittest)

    if ("$ENV{GTEST_HOME}" STREQUAL "")
        if (APPLE)
            set(GTEST_CMAKE_CXX_FLAGS "-fPIC -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
        else ()
            set(GTEST_CMAKE_CXX_FLAGS "-fPIC")
        endif ()
        string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)
        set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}} ${GTEST_CMAKE_CXX_FLAGS}")

        set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
        set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
        set(GTEST_STATIC_LIB
                "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX}")
        set(GTEST_MAIN_STATIC_LIB
                "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gmock_main${CMAKE_STATIC_LIBRARY_SUFFIX}")
        set(GTEST_VENDORED 1)
        set(GTEST_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                -DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}
                -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS})

        ExternalProject_Add(googletest_ep
                URL ${GTEST_SOURCE_URL}
                BUILD_BYPRODUCTS ${GTEST_STATIC_LIB} ${GTEST_MAIN_STATIC_LIB}
                CMAKE_ARGS ${GTEST_CMAKE_ARGS}
                ${EP_LOG_OPTIONS})
    else ()
        find_package(GTest REQUIRED)
        set(GTEST_VENDORED 0)
    endif ()

    message(STATUS "GTest include dir: ${GTEST_INCLUDE_DIR}")
    message(STATUS "GTest static library: ${GTEST_STATIC_LIB}")
    include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
    ADD_THIRDPARTY_LIB(gtest
            STATIC_LIB ${GTEST_STATIC_LIB})
    ADD_THIRDPARTY_LIB(gmock_main
            STATIC_LIB ${GTEST_MAIN_STATIC_LIB})

    if (GTEST_VENDORED)
        add_dependencies(gtest googletest_ep)
        add_dependencies(gmock_main googletest_ep)
    endif ()

    # gflags (formerly Googleflags) command line parsing
    if ("${GFLAGS_HOME}" STREQUAL "")
        set(GFLAGS_CMAKE_CXX_FLAGS ${EP_CXX_FLAGS})

        set(GFLAGS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gflags_ep-prefix/src/gflags_ep")
        set(GFLAGS_HOME "${GFLAGS_PREFIX}")
        set(GFLAGS_INCLUDE_DIR "${GFLAGS_PREFIX}/include")
        set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/libgflags.a")
        set(GFLAGS_VENDORED 1)
        set(GFLAGS_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                -DCMAKE_INSTALL_PREFIX=${GFLAGS_PREFIX}
                -DBUILD_SHARED_LIBS=OFF
                -DBUILD_STATIC_LIBS=ON
                -DBUILD_PACKAGING=OFF
                -DBUILD_TESTING=OFF
                -DBUILD_CONFIG_TESTS=OFF
                -DINSTALL_HEADERS=ON
                -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS}
                -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}
                -DCMAKE_CXX_FLAGS=${GFLAGS_CMAKE_CXX_FLAGS})

        ExternalProject_Add(gflags_ep
                URL ${GFLAGS_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                BUILD_IN_SOURCE 1
                BUILD_BYPRODUCTS "${GFLAGS_STATIC_LIB}"
                CMAKE_ARGS ${GFLAGS_CMAKE_ARGS})
    else ()
        set(GFLAGS_VENDORED 0)
        find_package(GFlags REQUIRED)
    endif ()

    message(STATUS "GFlags include dir: ${GFLAGS_INCLUDE_DIR}")
    message(STATUS "GFlags static library: ${GFLAGS_STATIC_LIB}")
    include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
    ADD_THIRDPARTY_LIB(gflags
            STATIC_LIB ${GFLAGS_STATIC_LIB})

    if (GFLAGS_VENDORED)
        add_dependencies(gflags gflags_ep)
    endif ()
endif ()

if (TERRIER_BUILD_BENCHMARKS)
    add_custom_target(runbenchmark ctest -L benchmark)

    if ("$ENV{GBENCHMARK_HOME}" STREQUAL "")
        set(GBENCHMARK_CMAKE_CXX_FLAGS "-fPIC -std=c++17 ${EP_CXX_FLAGS}")

        if (APPLE)
            set(GBENCHMARK_CMAKE_CXX_FLAGS "${GBENCHMARK_CMAKE_CXX_FLAGS} -stdlib=libc++")
        endif ()

        set(GBENCHMARK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
        set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
        set(GBENCHMARK_STATIC_LIB "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}")
        set(GBENCHMARK_VENDORED 1)
        set(GBENCHMARK_CMAKE_ARGS
                "-DCMAKE_BUILD_TYPE=Release"
                "-DCMAKE_INSTALL_PREFIX:PATH=${GBENCHMARK_PREFIX}"
                "-DBENCHMARK_ENABLE_TESTING=OFF"
                "-DCMAKE_CXX_FLAGS=${GBENCHMARK_CMAKE_CXX_FLAGS}")
        if (APPLE)
            set(GBENCHMARK_CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} "-DBENCHMARK_USE_LIBCXX=ON")
        endif ()

        ExternalProject_Add(gbenchmark_ep
                URL ${GBENCHMARK_SOURCE_URL}
                BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
                CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS}
                ${EP_LOG_OPTIONS})
    else ()
        find_package(GBenchmark REQUIRED)
        set(GBENCHMARK_VENDORED 0)
    endif ()

    message(STATUS "GBenchmark include dir: ${GBENCHMARK_INCLUDE_DIR}")
    message(STATUS "GBenchmark static library: ${GBENCHMARK_STATIC_LIB}")
    include_directories(SYSTEM ${GBENCHMARK_INCLUDE_DIR})
    ADD_THIRDPARTY_LIB(benchmark
            STATIC_LIB ${GBENCHMARK_STATIC_LIB})

    if (GBENCHMARK_VENDORED)
        add_dependencies(benchmark gbenchmark_ep)
    endif ()
endif ()

#----------------------------------------------------------------------

set(TERRIER_LINK_LIBS "")

# pthread https://cmake.org/cmake/help/latest/module/FindThreads.html
find_package(Threads REQUIRED)
list(APPEND TERRIER_LINK_LIBS ${CMAKE_THREAD_LIBS_INIT})

# JeMalloc
find_package(jemalloc REQUIRED)
include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})
if (TERRIER_USE_JEMALLOC)
    list(APPEND TERRIER_LINK_LIBS ${JEMALLOC_LIBRARIES})
    message(STATUS "jemalloc is enabled.")
endif ()

# ---[ Libevent
find_package(Libevent REQUIRED)
include_directories(SYSTEM ${LIBEVENT_INCLUDE_DIRS})
list(APPEND TERRIER_LINK_LIBS ${LIBEVENT_LIBRARIES})
list(APPEND TERRIER_LINK_LIBS ${LIBEVENT_PTHREAD_LIBRARIES})

# Intel TBB
find_package(TBB REQUIRED)
include_directories(SYSTEM ${TBB_INCLUDE_DIRS})
list(APPEND TERRIER_LINK_LIBS ${TBB_LIBRARIES})

# --[ PQXX
find_package(PQXX REQUIRED)
include_directories(SYSTEM ${PQXX_INCLUDE_DIRECTORIES})
list(APPEND TERRIER_LINK_LIBS ${PQXX_LIBRARIES})

# LLVM 7.0+
find_package(LLVM 7.0 REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "7.0")
  message(FATAL_ERROR "LLVM 7.0 or newer is required.")
endif ()
llvm_map_components_to_libnames(LLVM_LIBRARIES core mcjit nativecodegen native ipo)
include_directories(SYSTEM ${LLVM_INCLUDE_DIRS})
list(APPEND TERRIER_LINK_LIBS ${LLVM_LIBRARIES})

# Sqlite 3
find_package(Sqlite3 REQUIRED)
include_directories(SYSTEM ${SQLITE3_INCLUDE_DIRS})
list(APPEND TERRIER_LINK_LIBS ${SQLITE3_LIBRARIES})