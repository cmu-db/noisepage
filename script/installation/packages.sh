#!/bin/bash

## =================================================================
## TERRIER PACKAGE INSTALLATION
##
## This script will install all the packages that are needed to
## build and run the DBMS.
##
## Supported environments:
##  * Ubuntu 18.04
##  * macOS
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

give_up() {
    set +x
    OS=$1
    VERSION=$2
    [ ! -z "$VERSION" ] && VERSION=" $VERSION"
    
    echo
    echo "Unsupported distribution '${OS}${VERSION}'"
    echo "Please contact our support team for additional help."
    echo "Be sure to include the contents of this message."
    echo "Platform: $(uname -a)"
    echo
    echo "https://github.com/cmu-db/terrier/issues"
    echo
    exit 1
}

install() {
  set -x
  UNAME=$(uname | tr "[:lower:]" "[:upper:]" )
  VERSION=""

  case $UNAME in
    DARWIN) install_mac ;;

    LINUX)
      DISTRO=$(cat /etc/os-release | grep '^ID=' | cut -d '=' -f 2 | tr "[:lower:]" "[:upper:]" | tr -d '"')
      VERSION=$(cat /etc/os-release | grep '^VERSION_ID=' | cut -d '"' -f 2)
      
      # We only support Ubuntu right now
      [ "$DISTRO" != "UBUNTU" ] && give_up $DISTRO $VERSION
      
      # Check Ubuntu version
      case $VERSION in
        18.04) install_linux ;;
        *) give_up $DISTRO $VERSION;;
      esac
      ;;

    *) give_up $UNAME $VERSION;;
  esac
}

install_pip() {
  curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
  python get-pip.py
  rm get-pip.py
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
  brew ls --versions cmake || brew install cmake
  brew ls --versions coreutils || brew install coreutils
  brew ls --versions doxygen || brew install doxygen
  brew ls --versions git || brew install git
  brew ls --versions jemalloc || brew install jemalloc
  brew ls --versions libevent || brew install libevent
  brew ls --versions libpqxx || brew install libpqxx
  (brew ls --versions llvm@8 | grep 8) || brew install llvm@8
  brew ls --versions openssl@1.1 || brew install openssl@1.1
  brew ls --versions postgresql || brew install postgresql
  brew ls --versions tbb || brew install tbb
  brew ls --versions ant || brew install ant
  python3 -m pip --version || install_pip
  #install pyarrow
  python3 -m pip show pyarrow || python3 -m pip install pyarrow
  python3 -m pip show pandas || python3 -m pip install pandas
}

install_linux() {
  # Update apt-get.
  apt-get -y update
  
  # IMPORTANT: If you change anything listed below, you must also change it in the Dockerfile
  # in the root directory of the repository!
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
      ant \
      wget \
      python3-pip

  #install pyarrow
  python3 -m pip show pyarrow || python3 -m pip install pyarrow
  #install pandas
  python3 -m pip show pandas || python3 -m pip install pandas
         
  # IMPORTANT: Ubuntu 18.04 does not have libpqxx-6.2 available. So we have to download the package
  # manually and install it ourselves. We are *not* able to upgrade to libpqxx-6.4 because 18.04
  # does not have the right version of libstdc++6 that it needs.
  # Again, if you change the version make sure you update Dockerfile.
  LIBPQXX_VERSION="6.2.5-1"
  LIBPQXX_URL="http://mirrors.kernel.org/ubuntu/pool/universe/libp/libpqxx"
  LIBPQXX_FILES=(\
    "libpqxx-6.2_${LIBPQXX_VERSION}_amd64.deb" \
    "libpqxx-dev_${LIBPQXX_VERSION}_amd64.deb" \
  )
  for file in "${LIBPQXX_FILES[@]}"; do
    wget ${LIBPQXX_URL}/$file
    dpkg -i $file
    rm $file
  done
}

main "$@"
