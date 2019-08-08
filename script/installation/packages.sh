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
  brew install cmake
  brew install doxygen
  brew install git
  brew install jemalloc
  brew install libevent
  brew install libpqxx
  brew install llvm@8
  brew install openssl
  brew install postgresql
  brew install tbb
  brew install ant
}

install_linux() {
  # Update apt-get.
  apt-get -y update
  # Install packages.
  apt-get -y install \
      build-essential \
      clang-8 \
      clang-format-8 \
      clang-tidy-8 \
      cmake \
      doxygen \
      git \
      g++-7 \
      libevent-dev \
      libjemalloc-dev \
      libpq-dev \
      libssl-dev \
      libtbb-dev \
      zlib1g-dev \
      llvm-8 \
      pkg-config \
      postgresql-client \
      sqlite3 \
      libsqlite3-dev \
      ant
   #install libpqxx-6.2 manually
   apt-get -y install wget
   wget http://mirrors.kernel.org/ubuntu/pool/universe/libp/libpqxx/libpqxx-dev_6.2.4-4_amd64.deb
   wget http://mirrors.kernel.org/ubuntu/pool/universe/libp/libpqxx/libpqxx-6.2_6.2.4-4_amd64.deb
   dpkg -i libpqxx-6.2_6.2.4-4_amd64.deb
   dpkg -i libpqxx-dev_6.2.4-4_amd64.deb
   rm libpqxx-6.2_6.2.4-4_amd64.deb
   rm libpqxx-dev_6.2.4-4_amd64.deb
}

main "$@"
