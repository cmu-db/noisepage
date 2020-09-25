# Modified by the NoisePage project, originally from the xxhash repo.

# To the extent possible under law, the author(s) have dedicated all
# copyright and related and neighboring rights to this software to
# the public domain worldwide. This software is distributed without
# any warranty.
#
# For details, see <https://creativecommons.org/publicdomain/zero/1.0/>.

cmake_minimum_required (VERSION 2.8.12 FATAL_ERROR)

set(XXHASH_DIR "${CMAKE_CURRENT_SOURCE_DIR}/..")

file(STRINGS "${XXHASH_DIR}/xxhash.h" XXHASH_VERSION_MAJOR REGEX "^#define XXH_VERSION_MAJOR +([0-9]+) *$")
string(REGEX REPLACE "^#define XXH_VERSION_MAJOR +([0-9]+) *$" "\\1" XXHASH_VERSION_MAJOR "${XXHASH_VERSION_MAJOR}")
file(STRINGS "${XXHASH_DIR}/xxhash.h" XXHASH_VERSION_MINOR REGEX "^#define XXH_VERSION_MINOR +([0-9]+) *$")
string(REGEX REPLACE "^#define XXH_VERSION_MINOR +([0-9]+) *$" "\\1" XXHASH_VERSION_MINOR "${XXHASH_VERSION_MINOR}")
file(STRINGS "${XXHASH_DIR}/xxhash.h" XXHASH_VERSION_RELEASE REGEX "^#define XXH_VERSION_RELEASE +([0-9]+) *$")
string(REGEX REPLACE "^#define XXH_VERSION_RELEASE +([0-9]+) *$" "\\1" XXHASH_VERSION_RELEASE "${XXHASH_VERSION_RELEASE}")
set(XXHASH_VERSION_STRING "${XXHASH_VERSION_MAJOR}.${XXHASH_VERSION_MINOR}.${XXHASH_VERSION_RELEASE}")
set(XXHASH_LIB_VERSION ${XXHASH_VERSION_STRING})
set(XXHASH_LIB_SOVERSION "${XXHASH_VERSION_MAJOR}")
mark_as_advanced(XXHASH_VERSION_MAJOR XXHASH_VERSION_MINOR XXHASH_VERSION_RELEASE XXHASH_VERSION_STRING XXHASH_LIB_VERSION XXHASH_LIB_SOVERSION)

if("${CMAKE_VERSION}" VERSION_LESS "3.13")
    #message(WARNING "CMake ${CMAKE_VERSION} has no CMP0077 policy: options will erase uncached/untyped normal vars!")
else()
    cmake_policy (SET CMP0077 NEW)
endif()
if("${CMAKE_VERSION}" VERSION_LESS "3.0")
    project(xxHash C)
else()
    cmake_policy (SET CMP0048 NEW)
    project(xxHash
        VERSION ${XXHASH_VERSION_STRING}
        LANGUAGES C)
endif()

# libxxhash
add_library(xxhash "${XXHASH_DIR}/xxhash.c")
add_library(${PROJECT_NAME}::xxhash ALIAS xxhash)

target_include_directories(xxhash
  SYSTEM PUBLIC
    $<BUILD_INTERFACE:${XXHASH_DIR}>
    $<INSTALL_INTERFACE:include/>)

set_target_properties(xxhash PROPERTIES
  SOVERSION "${XXHASH_VERSION_STRING}"
  VERSION "${XXHASH_VERSION_STRING}")
