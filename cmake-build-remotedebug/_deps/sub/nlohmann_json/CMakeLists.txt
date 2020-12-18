# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.17.3)

# We name the project and the target for the ExternalProject_Add() call
# to something that will highlight to the user what we are working on if
# something goes wrong and an error message is produced.

project(nlohmann_json-populate NONE)

include(ExternalProject)
ExternalProject_Add(nlohmann_json-populate
                     "GIT_REPOSITORY" "https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git" "GIT_TAG" "v3.7.3"
                    SOURCE_DIR          "/Users/ejeppinger/CLionProjects/terrier/cmake-build-remotedebug/_deps/src/nlohmann_json"
                    BINARY_DIR          "/Users/ejeppinger/CLionProjects/terrier/cmake-build-remotedebug/_deps/build/nlohmann_json"
                    CONFIGURE_COMMAND   ""
                    BUILD_COMMAND       ""
                    INSTALL_COMMAND     ""
                    TEST_COMMAND        ""
                    USES_TERMINAL_DOWNLOAD  YES
                    USES_TERMINAL_UPDATE    YES
)
