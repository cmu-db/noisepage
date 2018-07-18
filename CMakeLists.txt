project(terrier)
# ---------------------------------------------------------
# CMake
# ---------------------------------------------------------

cmake_minimum_required(VERSION 3.10)
include(CheckCXXCompilerFlag)
include(CTest)

# ---[ https://cmake.org/cmake/help/v3.1/policy/CMP0054.html
if(POLICY CMP0054)
    cmake_policy(SET CMP0054 NEW)
endif()

# ---------------------------------------------------------
# Project
# ---------------------------------------------------------

include(cmake/Utils.cmake)

# ---[ Terrier version
set(TERRIER_TARGET_VERSION "0.0.1" CACHE STRING "Terrier logical version")
set(TERRIER_TARGET_SOVERSION "0.0.1" CACHE STRING "Terrier soname version")
add_definitions(-DTERRIER_VERSION=${TERRIER_TARGET_VERSION})

# ---[ Output directory
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# ---[ Using cmake scripts and modules
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

# ---[ Documentation
terrier_option(BUILD_docs   "Build documentation" ON IF UNIX OR APPLE)

# CodeInspection.cmake has to be put after Dependencies.cmake in the include order
include(cmake/Compiler.cmake)
include(cmake/CompilerFlags.cmake)
include(cmake/Sanitizers.cmake)
include(cmake/Doxygen.cmake)
include(cmake/Dependencies.cmake)
include(cmake/Targets.cmake)
include(cmake/ConfigGen.cmake)
include(cmake/CodeInspection.cmake)

# ---[ Config generation
configure_file(cmake/Templates/terrier_config.h.in "${PROJECT_BINARY_DIR}/terrier_config.h")

# ---[ Includes
set(TERRIER_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(TERRIER_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(TERRIER_THIRD_PARTY_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party)
include_directories(${TERRIER_SRC_INCLUDE_DIR} ${TERRIER_TEST_INCLUDE_DIR} ${TERRIER_THIRD_PARTY_INCLUDE_DIR} ${PROJECT_BINARY_DIR})

# ---[ Subdirectories
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(third_party/googletest)
add_subdirectory(third_party/jsoncpp)