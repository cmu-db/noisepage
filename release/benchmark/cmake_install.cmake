# Install script for directory: /Users/jeffniu/Desktop/terrier/benchmark

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "RELEASE")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/Users/jeffniu/Desktop/terrier/release/benchmark/catalog/cmake_install.cmake")
  include("/Users/jeffniu/Desktop/terrier/release/benchmark/integration/cmake_install.cmake")
  include("/Users/jeffniu/Desktop/terrier/release/benchmark/metrics/cmake_install.cmake")
  include("/Users/jeffniu/Desktop/terrier/release/benchmark/parser/cmake_install.cmake")
  include("/Users/jeffniu/Desktop/terrier/release/benchmark/storage/cmake_install.cmake")
  include("/Users/jeffniu/Desktop/terrier/release/benchmark/transaction/cmake_install.cmake")
  include("/Users/jeffniu/Desktop/terrier/release/benchmark/runner/cmake_install.cmake")

endif()

