#!/bin/bash

## =================================================================
## TERRIER PACKAGE INSTALLATION
##
## This script will install all the packages that are needed to
## build and run the DBMS.
##
## Supported environments:
##  * Ubuntu 20.04
##  * macOS
## =================================================================

# IMPORTANT: We special case the handling of the llvm package to make sure 
# to get the correct version that we want. So it is not included in this list.
# See the 'install_mac' function below.
OSX_BUILD_PACKAGES=(\
  "cmake" \
  "coreutils" \
  "doxygen" \
  "git" \
  "jemalloc" \
  "libevent" \
  "libpqxx" \
  "openssl@1.1" \
  "tbb" \
)
OSX_TEST_PACKAGES=(\
  "ant" \
  "postgresql" \
  "lsof" \
)

LINUX_BUILD_PACKAGES=(\
  "build-essential" \
  "clang-8" \
  "clang-format-8" \
  "clang-tidy-8" \
  "cmake" \
  "doxygen" \
  "git" \
  "libevent-dev" \
  "libjemalloc-dev" \
  "libpq-dev" \
  "libpqxx-dev" \
  "libssl-dev" \
  "libtbb-dev" \
  "zlib1g-dev" \
  "llvm-8" \
  "pkg-config" \
  "postgresql-client" \
  "wget" \
  "python3-pip" \
)
LINUX_TEST_PACKAGES=(\
  "ant" \
  "curl" \
  "lcov" \
  "ccache" \
  "lsof" \
)

# These are the packages that we will install with pip3
# We will install these for both build and test.
PYTHON_PACKAGES=(\
  "pyarrow" \
  "pandas" \
  "requests" \
  "distro"  \
)


## =================================================================


main() {
  set -o errexit

  INSTALL_TYPE="$1"
  if [ -z "$INSTALL_TYPE" ]; then
    INSTALL_TYPE="build"
  fi
  ALLOWED=("build" "test" "all")
  FOUND=0
  for key in "${ALLOWED[@]}"; do
    if [ "$key" == "$INSTALL_TYPE" ] ; then
      FOUND=1
    fi
  done
  if [ "$FOUND" = "0" ]; then
    echo "Invalid installation type '$INSTALL_TYPE'"
    echo -n "Allowed Values: "
    ( IFS=$' '; echo "${ALLOWED[*]}" )
    exit 1
  fi
  
  echo "PACKAGES WILL BE INSTALLED. THIS MAY BREAK YOUR EXISTING TOOLCHAIN."
  echo "YOU ACCEPT ALL RESPONSIBILITY BY PROCEEDING."
  echo
  echo "INSTALLATION TYPE: $INSTALL_TYPE"
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
        20.04) install_linux ;;
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
  if [ "$INSTALL_TYPE" == "build" -o "$INSTALL_TYPE" = "all" ]; then
    for pkg in "${OSX_BUILD_PACKAGES[@]}"; do
      brew ls --versions $pkg || brew install $pkg
    done
  fi
  if [ "$INSTALL_TYPE" == "test" -o "$INSTALL_TYPE" = "all" ]; then
    for pkg in "${OSX_TEST_PACKAGES[@]}"; do
      brew ls --versions $pkg || brew install $pkg
    done
  fi
  
  # Special case for llvm
  (brew ls --versions llvm@8 | grep 8) || brew install llvm@8
  
  # Always install Python stuff
  python3 -m pip --version || install_pip
  for pkg in "${PYTHON_PACKAGES[@]}"; do
    python3 -m pip show $pkg || python3 -m pip install $pkg
  done
}

install_linux() {
  # Update apt-get.
  apt-get -y update
  
  # Install packages.
  if [ "$INSTALL_TYPE" == "build" -o "$INSTALL_TYPE" = "all" ]; then
    apt-get -y install `( IFS=$' '; echo "${LINUX_BUILD_PACKAGES[*]}" )`
  fi
  if [ "$INSTALL_TYPE" == "test" -o "$INSTALL_TYPE" = "all" ]; then
    apt-get -y install `( IFS=$' '; echo "${LINUX_TEST_PACKAGES[*]}" )`
  fi
  
  # Always install Python stuff
  # python3 -m pip --version || install_pip
  for pkg in "${PYTHON_PACKAGES[@]}"; do
    python3 -m pip show $pkg || python3 -m pip install $pkg
  done
}

main "$@"
