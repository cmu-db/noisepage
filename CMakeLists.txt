project(terrier)

# ---------------------------------------------------------
# CMAKE
# ---------------------------------------------------------

cmake_minimum_required(VERSION 3.10)
include(CheckCXXCompilerFlag)

# ---[ https://cmake.org/cmake/help/v3.1/policy/CMP0054.html
if(POLICY CMP0054)
    cmake_policy(SET CMP0054 NEW)
endif()

# ---[ Output directory
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# ---[ Using cmake scripts and modules
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

include(ExternalProject)
include(cmake/Compiler.cmake)
include(cmake/CompilerFlags.cmake)
include(cmake/Sanitizers.cmake)
include(cmake/Dependencies.cmake)
include(cmake/Targets.cmake)
include(cmake/Utils.cmake)
include(cmake/ConfigGen.cmake)

# ---[ Config generation
configure_file(cmake/Templates/terrier_config.h.in "${PROJECT_BINARY_DIR}/terrier_config.h")

# ---[ Includes
set(terrier_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(terrier_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(terrier_THIRD_PARTY_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party)
include_directories(${terrier_SRC_INCLUDE_DIR} ${terrier_TEST_INCLUDE_DIR} ${terrier_THIRD_PARTY_INCLUDE_DIR} ${PROJECT_BINARY_DIR})
include_directories(BEFORE src) # This is needed for gtest.

# ---[ Subdirectories
add_subdirectory(src)

terrier_generate_export_configs()