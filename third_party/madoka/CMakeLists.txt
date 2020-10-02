cmake_minimum_required(VERSION 3.16)

project(madoka
    VERSION 0.1.0.0
    DESCRIPTION "NoisePage trying to use madoka."
    LANGUAGES CXX
)

# Compile all the .c files.
file(GLOB_RECURSE
    MADOKA_SRCS
    CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/*.h
        ${CMAKE_CURRENT_SOURCE_DIR}/*.cc
)

# Create the madoka library along with a namespaced alias.
add_library(madoka STATIC "${MADOKA_SRCS}")
add_library(${PROJECT_NAME}::madoka ALIAS madoka)

# Specify all the necessary includes.
target_include_directories(
    madoka
    SYSTEM PUBLIC
        # These includes are exposed to the users of this library.
        ${CMAKE_CURRENT_SOURCE_DIR}/..
    PRIVATE
        # These includes are library-internal and necessary for compilation.
        ${CMAKE_CURRENT_SOURCE_DIR}
)

set_target_properties(madoka PROPERTIES POSITION_INDEPENDENT_CODE ON)
