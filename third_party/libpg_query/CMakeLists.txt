cmake_minimum_required(VERSION 3.16)

project(pg_query
        VERSION 0.1.0.0
        DESCRIPTION "NoisePage trying to use libpg_query."
        LANGUAGES CXX C
        )

# Compile all the .c files.
file(GLOB_RECURSE
        PG_QUERY_SRCS
        CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
        )
# TODO(WAN): I have no clue why these were removed, or if there are more that can be removed. Probably yes.
list(REMOVE_ITEM
        PG_QUERY_SRCS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_fingerprint_defs.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_fingerprint_conds.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_json_defs.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_json_conds.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/postgres/guc-file.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/postgres/scan.c"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/pg_query_json_helper.c"
        )

# Create the pg_query library along with a namespaced alias.
add_library(pg_query STATIC "${PG_QUERY_SRCS}")
add_library(${PROJECT_NAME}::pg_query ALIAS pg_query)

# Specify all the necessary includes.
target_include_directories(
        pg_query
        SYSTEM PUBLIC
        # These includes are exposed to the users of this library.
        ${CMAKE_CURRENT_SOURCE_DIR}/../src/postgres/include
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        PRIVATE
        # These includes are library-internal and necessary for compilation.
        ${CMAKE_CURRENT_SOURCE_DIR}/src/postgres/include
        ${CMAKE_CURRENT_SOURCE_DIR}
)

set_target_properties(pg_query PROPERTIES LINKER_LANGUAGE C)
set_target_properties(pg_query PROPERTIES POSITION_INDEPENDENT_CODE ON)
