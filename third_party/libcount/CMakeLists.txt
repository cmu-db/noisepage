cmake_minimum_required(VERSION 3.16)

project(libcount
    VERSION 0.1.0.0
    DESCRIPTION "NoisePage trying to use libcount."
    LANGUAGES CXX
)

# Create the libcount library along with a namespaced alias.
add_library(libcount STATIC c.cc empirical_data.cc hll.cc hll_data.cc)
add_library(${PROJECT_NAME}::libcount ALIAS libcount)

# Specify all the necessary includes.
target_include_directories(
    libcount
    SYSTEM PUBLIC
        # These includes are exposed to the users of this library.
        ${CMAKE_CURRENT_SOURCE_DIR}/..
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)
