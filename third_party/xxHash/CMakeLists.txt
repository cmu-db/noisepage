cmake_minimum_required(VERSION 3.16)

project(xxHash
        VERSION 0.1.0.0
        DESCRIPTION "NoisePage trying to use xxHash."
        LANGUAGES C
        )

# Compile all the .c files.
file(GLOB_RECURSE
        XXHASH_SRCS
        CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h
        ${CMAKE_CURRENT_SOURCE_DIR}/*.c
        )

# Create the pg_query library along with a namespaced alias.
add_library(xxHash STATIC "${XXHASH_SRCS}")
add_library(${PROJECT_NAME}::xxHash ALIAS xxHash)

# Specify all the necessary includes.
target_include_directories(
        xxHash
        SYSTEM PUBLIC
        # These includes are exposed to the users of this library.
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        PRIVATE
        # These includes are library-internal and necessary for compilation.
        ${CMAKE_CURRENT_SOURCE_DIR}
)

set_target_properties(pg_query PROPERTIES LINKER_LANGUAGE C)
set_target_properties(pg_query PROPERTIES POSITION_INDEPENDENT_CODE ON)
