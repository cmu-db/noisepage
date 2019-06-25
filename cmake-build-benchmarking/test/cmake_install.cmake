# Install script for directory: /Users/vivianhuang/Desktop/terrier/test

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
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/common/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/network/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/parser/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/storage/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/transaction/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/type/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/settings/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/planner/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/optimizer/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/traffic_cop/cmake_install.cmake")
  include("/Users/vivianhuang/Desktop/terrier/cmake-build-benchmarking/test/integration/cmake_install.cmake")

endif()

