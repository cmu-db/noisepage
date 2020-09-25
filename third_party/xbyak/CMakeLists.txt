cmake_minimum_required(VERSION 3.16)

project(xbyak
    VERSION 0.1.0.0
    DESCRIPTION "NoisePage trying to use xbyak."
    LANGUAGES CXX
)

# Create the pg_query library along with a namespaced alias.
add_library(xbyak INTERFACE)
add_library(${PROJECT_NAME}::xbyak ALIAS xbyak)

# Specify all the necessary includes.
target_include_directories(
    xbyak
    INTERFACE
        # These includes are exposed to the users of this library.
        ${CMAKE_CURRENT_SOURCE_DIR}/..
)
