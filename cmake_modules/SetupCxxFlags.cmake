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

# Check if the target architecture and compiler supports some special
# instruction sets that would boost performance.
include(CheckCXXCompilerFlag)

# compiler flags that are common across debug/release builds
set(CXX_COMMON_FLAGS "-Wall -Werror")
set(CXX_ONLY_FLAGS "-std=c++17 -fPIC -mcx16 -march=native -fvisibility=hidden")
set(CLANG_CXX_FLAGS "-Wno-c++98-compat -Wno-c++98-compat-pedantic -Qunused-arguments -Wno-unknown-warning-option -fcolor-diagnostics")
set(GCC_CXX_FLAGS "-Wconversion -Wno-sign-conversion -fdiagnostics-color=auto")

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For all builds:
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=FastDebug
#   Same as DEBUG, except with some optimizations on.
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   Debug symbols are stripped for reduced binary size. Add
#   -DTERRIER_CXXFLAGS="-g" to add them
# For CMAKE_BUILD_TYPE=RelWithDebInfo
#   -O2: Some compiler optimizations
#   -ggdb: Enable gdb debugging
set(CXX_FLAGS_DEBUG "-ggdb -O0 -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CXX_FLAGS_FASTDEBUG "-ggdb -O1 -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CXX_FLAGS_RELWITHDEBINFO "-ggdb -O2 -DNDEBUG")

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif (NOT CMAKE_BUILD_TYPE)

string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

# Set compile flags based on the build type.
message("Configured for ${CMAKE_BUILD_TYPE} build (set with cmake -DCMAKE_BUILD_TYPE={release,debug,fastdebug,relwithdebinfo})")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(CXX_OPTIMIZATION_FLAGS "${CXX_FLAGS_DEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
    set(CXX_OPTIMIZATION_FLAGS "${CXX_FLAGS_FASTDEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    set(CXX_OPTIMIZATION_FLAGS "${CXX_FLAGS_RELEASE}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELWITHDEBINFO")
    set(CXX_OPTIMIZATION_FLAGS "${CXX_FLAGS_RELWITHDEBINFO}")
else ()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

set(CMAKE_CXX_FLAGS "${CXX_OPTIMIZATION_FLAGS}")
