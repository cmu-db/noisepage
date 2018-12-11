#!/bin/bash

## =================================================================
## TERRIER PACKAGE INSTALLATION
##
## This script will install all the packages that are needed to
## build and run the DBMS.
##
## Note: On newer versions of Ubuntu (17.04), this script
## will not install the correct version of g++. You will have
## to use 'update-alternatives' to configure the default of
## g++ manually.
##
## Supported environments:
##  * Ubuntu 18.04
##  * MacOS
## =================================================================

main() {
  set -o errexit

    echo "PACKAGES WILL BE INSTALLED. THIS MAY BREAK YOUR EXISTING TOOLCHAIN."
    echo "YOU ACCEPT ALL RESPONSIBILITY BY PROCEEDING."
    read -p "Proceed? [Y/n] : " yn
    case $yn in
        Y|y) install;;
        *) ;;
    esac

    echo "Script complete."
}

install() {
  set -x
  UNAME=$(uname | tr "[:lower:]" "[:upper:]" )

  case $UNAME in
    DARWIN) install_mac ;;

    LINUX)
      version=$(cat /etc/os-release | grep VERSION_ID | cut -d '"' -f 2)
      case $version in
        18.04) install_linux ;;
        *) give_up ;;
      esac
      ;;

    *) give_up ;;
  esac
}

give_up() {
  set +x
  echo "Unsupported distribution '$UNAME'"
  echo "Please contact our support team for additional help."
  echo "Be sure to include the contents of this message."
  echo "Platform: $(uname -a)"
  echo
  echo "https://github.com/cmu-db/terrier/issues"
  echo
  exit 1
}

install_mac() {
  # Install Homebrew.
  if test ! $(which brew); then
    echo "Installing Homebrew (https://brew.sh/)"
    ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
  fi
  # Update Homebrew.
  brew update
  # Install packages.
  brew ls --versions boost || brew install boost
  brew ls --versions cmake || brew install cmake
  brew ls --versions doxygen || brew install doxygen
  brew ls --versions git || brew install git
  brew ls --versions jemalloc || brew install jemalloc
  brew ls --versions libevent || brew install libevent
  (brew ls --versions llvm | grep 6) || brew install llvm@6
  brew ls --versions tbb || brew install tbb
}

install_linux() {
  # Update apt-get.
  apt-get -y update
  # Install packages.
  apt-get -y install \
      build-essential \
      clang-6.0 \
      clang-format-6.0 \
      clang-tidy-6.0 \
      cmake \
      doxygen \
      git \
      g++-7 \
      libboost-filesystem-dev \
      llibevent-dev \
      libjemalloc-dev \
      libtbb-dev \
      libz-dev \
      llvm-6.0
}

main "$@"
