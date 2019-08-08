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

cmake_minimum_required(VERSION 3.2)

# Extract Terrier version number
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/build-support/version.xml" POM_XML)
string(REGEX MATCHALL
        "\n<version>[^<]+</version>" TERRIER_VERSION_TAG "${POM_XML}")
string(REGEX REPLACE
        "(\n<version>|</version>)" "" TERRIER_VERSION "${TERRIER_VERSION_TAG}")
string(REGEX MATCH
        "^[0-9]+\\.[0-9]+\\.[0-9]+" TERRIER_BASE_VERSION "${TERRIER_VERSION}")

project(terrier
        LANGUAGES CXX
        VERSION "${TERRIER_BASE_VERSION}"
        )

set(TERRIER_VERSION_MAJOR "${terrier_VERSION_MAJOR}")
set(TERRIER_VERSION_MINOR "${terrier_VERSION_MINOR}")
set(TERRIER_VERSION_PATCH "${terrier_VERSION_PATCH}")
if (TERRIER_VERSION_MAJOR STREQUAL "" OR
        TERRIER_VERSION_MINOR STREQUAL "" OR
        TERRIER_VERSION_PATCH STREQUAL "")
    MESSAGE(FATAL_ERROR "Failed to determine Terrier version from '${TERRIER_VERSION}'")
endif ()

# The SO version is also the ABI version
# Terrier 0.x.y => SO version is "x", full SO version is "x.y.0"
set(TERRIER_SO_VERSION "${TERRIER_VERSION_MINOR}")
set(TERRIER_FULL_SO_VERSION "${TERRIER_SO_VERSION}.${TERRIER_VERSION_PATCH}.0")

message(STATUS "Terrier version: "
        "${TERRIER_VERSION_MAJOR}.${TERRIER_VERSION_MINOR}.${TERRIER_VERSION_PATCH} "
        "(full: '${TERRIER_VERSION}')")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

include(CMakeParseArguments)
include(ExternalProject)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)

# Compatibility with CMake 3.1
if (POLICY CMP0054)
    # http://www.cmake.org/cmake/help/v3.1/policy/CMP0054.html
    cmake_policy(SET CMP0054 NEW)
endif ()

if (APPLE)
    cmake_policy(SET CMP0068 NEW)
endif ()

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

###################################################
# CMAKE OPTIONS
###################################################

set(TERRIER_CXXFLAGS "" CACHE STRING
        "Compiler flags to append when compiling Terrier")

option(TERRIER_BUILD_STATIC
        "Build the libterrier static libraries"
        ON)

option(TERRIER_BUILD_SHARED
        "Build the libterrier shared libraries"
        ON)

option(TERRIER_TEST_MEMCHECK
        "Run the test suite using valgrind --tool=memcheck"
        OFF)

option(TERRIER_BUILD_TESTS
        "Build the Terrier googletest unit tests"
        ON)

option(TERRIER_BUILD_BENCHMARKS
        "Build the Terrier micro benchmarks"
        ON)

option(TERRIER_RPATH_ORIGIN
        "Build Terrier libraries with RATH set to \$ORIGIN"
        OFF)

option(TERRIER_INSTALL_NAME_RPATH
        "Build Terrier libraries with install_name set to @rpath"
        ON)

option(TERRIER_GENERATE_COVERAGE
        "Build with C++ code coverage enabled"
        OFF)

option(TERRIER_USE_JEMALLOC
        "Dynamically link jemalloc as the memory allocator."
        OFF)

option(TERRIER_VERBOSE_THIRDPARTY_BUILD
        "If off, output from ExternalProjects will be logged to files rather than shown"
        OFF)

option(TERRIER_VERBOSE_LINT
        "If off, 'quiet' flags will be passed to linting tools"
        OFF)

if (NOT TERRIER_BUILD_TESTS)
    set(NO_TESTS 1)
endif ()

if (NOT TERRIER_BUILD_BENCHMARKS)
    set(NO_BENCHMARKS 1)
endif ()

###################################################
# COMPILER TOOLS
###################################################

set(CLANG_TOOLS_VERSION "7")
find_package(ClangTools)
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
    # Generate a Clang compile_commands.json "compilation database" file for use
    # with various development tools, such as Vim's YouCompleteMe plugin.
    # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
    set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif ()

# data directory for SSL
set(DATA_DIR "${PROJECT_SOURCE_DIR}/data")

############################################################
# Compiler flags
############################################################

# Determine compiler version
include(CompilerInfo)
include(SetupCxxFlags)

############################################################
# Dependencies
############################################################

include(BuildUtils)
enable_testing()

include(ThirdpartyToolchain)

# Add common flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_COMMON_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TERRIER_CXXFLAGS}")

# For any C code, use the same flags.
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")

# Remove --std=c++17 to avoid errors from C compilers
string(REPLACE "-std=c++17" "" CMAKE_C_FLAGS ${CMAKE_C_FLAGS})

# Add C++-only flags, like -std=c++17
set(CMAKE_CXX_FLAGS "${CXX_ONLY_FLAGS} ${CMAKE_CXX_FLAGS}")

# ASAN / TSAN / UBSAN
include(san-config)

###########################################################
# code coverage
###########################################################

if (TERRIER_GENERATE_COVERAGE)

    if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
        message(FATAL_ERROR "Coverage can only be generated with a debug build type!")
    endif ()

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage")
endif ()

# CMAKE_CXX_FLAGS now fully assembled
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

###########################################################
# "make check-lint" and "make check-censored" targets
###########################################################
if (NOT TERRIER_VERBOSE_LINT)
    set(TERRIER_LINT_QUIET "--quiet")
endif ()

if (UNIX)

    file(GLOB_RECURSE LINT_FILES
            "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
            "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
            "${CMAKE_CURRENT_SOURCE_DIR}/test/*.h"
            "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp"
            "${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.h"
            "${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.cpp"
            )

    find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
    message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

    # Full lint
    # Balancing act: cpplint.py takes a non-trivial time to launch,
    # so process 12 files per invocation, while still ensuring parallelism
    add_custom_target(check-lint echo '${LINT_FILES}' | xargs -n12 -P8
            ${CPPLINT_BIN}
            --verbose=2 ${TERRIER_LINT_QUIET}
            --linelength=120
            --filter=-legal/copyright,-build/header_guard
            )

    set(CENSOR_FILES ${LINT_FILES})

    # If whitelisting becomes necessary, do it here. Make it harder so that people try not to do it.
    #  list(REMOVE_ITEM CENSOR_FILES
    #
    #      )

    add_custom_target(check-censored
            grep --invert-match -n -e '^ *//' -e '^ *[*]' ${CENSOR_FILES} # check all uncommented lines w/ line num
            | grep -i -f ${BUILD_SUPPORT_DIR}/bad_words.txt               # for bad words, not case sensitive
            | grep --invert-match -e 'NOLINT'                             # the count of them that aren't NOLINT
            || exit 0                                                     # if nothing found, return 0
            && exit 1                                                     # else return 1, note || && left-associative
            )
endif (UNIX)

###########################################################
# "make format" and "make check-format" targets
###########################################################

# we modified the format script to take multiple args

string(CONCAT FORMAT_DIRS
        "${CMAKE_CURRENT_SOURCE_DIR}/benchmark,"
        "${CMAKE_CURRENT_SOURCE_DIR}/src,"
        "${CMAKE_CURRENT_SOURCE_DIR}/test,"
        )

# runs clang format and updates files in place.
add_custom_target(format ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        --source_dirs
        ${FORMAT_DIRS}
        --fix
        ${TERRIER_LINT_QUIET})

# runs clang format and exits with a non-zero exit code if any files need to be reformatted
add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        --source_dirs
        ${FORMAT_DIRS}
        ${TERRIER_LINT_QUIET})

###########################################################
# "make check-clang-tidy" target
###########################################################
if (${CLANG_TIDY_FOUND})
    # runs clang-tidy and exits with a non-zero exit code if any errors are found.
    # note that clang-tidy automatically looks for a .clang-tidy file in parent directories
    add_custom_target(check-clang-tidy
            ${BUILD_SUPPORT_DIR}/run-clang-tidy.py                            # run LLVM's clang-tidy script
            -clang-tidy-binary ${CLANG_TIDY_BIN}                              # using our clang-tidy binary
            -p ${CMAKE_BINARY_DIR}                                            # using cmake's generated compile commands
            )
    if (TERRIER_BUILD_BENCHMARKS)
        add_dependencies(check-clang-tidy benchmark gtest gflags)               # clang-tidy needs their headers to exist
    elseif (TERRIER_BUILD_TESTS)
        add_dependencies(check-clang-tidy gtest gflags)                         # clang-tidy needs their headers to exist
    endif ()

endif ()

###########################################################
# Build properties
###########################################################

# set compile output directory
string(TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}/")
    # Link build/latest to the current build directory, to avoid developers
    # accidentally running the latest debug build when in fact they're building
    # release builds.
    FILE(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
    if (NOT APPLE)
        set(MORE_ARGS "-T")
    endif ()
    EXECUTE_PROCESS(COMMAND ln ${MORE_ARGS} -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
            ${CMAKE_CURRENT_BINARY_DIR}/build/latest)
else ()
    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}/")
endif ()

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

include_directories(${PROJECT_SOURCE_DIR}/src/include)
if (${COMPILER_FAMILY} STREQUAL "gcc")
    include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third_party)
else ()
    include_directories(${PROJECT_SOURCE_DIR}/third_party)
endif ()
include_directories(${PROJECT_SOURCE_DIR}/test/include)
include_directories(${PROJECT_SOURCE_DIR}/benchmark/include)
include_directories(${PROJECT_BINARY_DIR})

# needed for BwTree
if (CMAKE_COMPILER_IS_GNUCXX)
    set(TERRIER_LINK_LIBS ${TERRIER_LINK_LIBS} atomic)
endif ()

set(TERRIER_LINK_LIBS
        gflags
        ${TERRIER_LINK_LIBS}
        pg_query)

set(TERRIER_DEPENDENCIES
        gflags)

set(TERRIER_TEST_LINK_LIBS
        test_util
        terrier_static
        gtest
        gmock_main
        gflags
        ${CMAKE_DL_LIBS})

set(TERRIER_BENCHMARK_LINK_LIBS
        benchmark_util
        test_util
        terrier_static
        benchmark
        gtest
        gmock_main
        gflags
        ${CMAKE_DL_LIBS})

# ---[ Subdirectories
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(benchmark)

# xxHash (for TPL)
add_subdirectory(third_party/xxHash/cmake_unofficial)
