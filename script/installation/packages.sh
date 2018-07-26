#!/bin/bash -x

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
##  * Ubuntu (14.04, 18.04)
##  * macOS
## =================================================================

set -o errexit

# Determine OS platform
UNAME=$(uname | tr "[:upper:]" "[:lower:]")
# If Linux, try to determine specific distribution
if [ "$UNAME" == "linux" ]; then
    # If available, use LSB to identify distribution
    if [ -f /etc/lsb-release -o -d /etc/lsb-release.d ]; then
        export DISTRO=$(lsb_release -is)
        DISTRO_VER=$(lsb_release -rs)
    # Otherwise, use release info file
    else
        export DISTRO=$(ls -d /etc/[A-Za-z]*[_-][rv]e[lr]* | grep -v "lsb" | cut -d'/' -f3 | cut -d'-' -f1 | cut -d'_' -f1)
        DISTRO_VER=$(cat /etc/*-release | grep "VERSION_ID" | cut -d'=' -f2 | tr -d '"')
    fi
fi
# For everything else (or if above failed), just use generic identifier
[ "$DISTRO" == "" ] && export DISTRO=$UNAME
unset UNAME
DISTRO=$(echo $DISTRO | tr "[:lower:]" "[:upper:]")

## ------------------------------------------------
## UBUNTU
## ------------------------------------------------
if [ "$DISTRO" = "UBUNTU" ]; then
    MAJOR_VER=$(echo "$DISTRO_VER" | cut -d '.' -f 1)
    # Fix for LLVM-3.7 on Ubuntu 14 + 17
    if [ "$MAJOR_VER" == "14" -o "$MAJOR_VER" == "18" ]; then
        if [ "$MAJOR_VER" == "14" ]; then
            LLVM_PKG_URL="http://llvm.org/apt/trusty/"
            LLVM_PKG_TARGET="llvm-toolchain-trusty-6.0 main"
            if ! grep -q "deb $LLVM_PKG_URL $LLVM_PKG_TARGET" /etc/apt/sources.list; then
                echo -e "\n# Added by Terrier 'packages.sh' script on $(date)\ndeb $LLVM_PKG_URL $LLVM_PKG_TARGET\ndeb http://ppa.launchpad.net/ubuntu-toolchain-r/test/ubuntu trusty main" | sudo tee -a /etc/apt/sources.list > /dev/null
            fi
        fi
        if [ "$MAJOR_VER" == "18" ]; then
            LLVM_PKG_URL="http://apt.llvm.org/bionic/"
            LLVM_PKG_TARGET="llvm-toolchain-bionic-6.0 main"
            if ! grep -q "deb $LLVM_PKG_URL $LLVM_PKG_TARGET" /etc/apt/sources.list; then
                echo -e "\n# Added by Terrier 'packages.sh' script on $(date)\ndeb $LLVM_PKG_URL $LLVM_PKG_TARGET" | sudo tee -a /etc/apt/sources.list > /dev/null
            fi
        fi

        sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 15CF4D18AF4F7421
    fi

    sudo apt-get update
    FORCE_Y=""
    PKG_CMAKE="cmake"
    PKG_LLVM="llvm"
    PKG_CLANG="clang"

    # Fix for cmake name change on Ubuntu 14.x --force-yes deprecation
    if [ "$MAJOR_VER" == "14" ]; then
        FORCE_Y="--force-yes"
        PKG_CMAKE="cmake3"
        PKG_LLVM="llvm-6.0"
        PKG_CLANG="clang-6.0"
    fi
    sudo apt-get -q $FORCE_Y --ignore-missing -y install \
        $PKG_CMAKE \
        $PKG_LLVM \
        $PKG_CLANG \
        git \
        g++ \
        valgrind \
        lcov \
        libgflags-dev \
        libboost-dev \
        libboost-thread-dev \
        libboost-filesystem-dev \
        libjemalloc-dev \
        libjsoncpp-dev \
        libtbb-dev \
        python3-pip \
        curl \
        autoconf \
        automake \
        libtool \
        make \
        zlib1g-dev

## ------------------------------------------------
## DARWIN (macOS)
## ------------------------------------------------
elif [ "$DISTRO" = "DARWIN" ]; then
    set +o errexit
    if test ! $(which brew); then
      echo "Installing homebrew..."
      ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    fi
    brew install boost
    brew install cmake
    brew install curl
    brew install git
    brew install gflags
    brew install jemalloc
    brew install jsoncpp
    brew install lcov
    brew install llvm
    brew install python
    brew upgrade python
    brew install tbb
    brew install wget

## ------------------------------------------------
## UNKNOWN
## ------------------------------------------------
else
    echo "Unsupported distribution '$DISTRO'"
    echo "Please contact our support team for additional help." \
         "Be sure to include the contents of this message"
    echo "Platform: $(uname -a)"
    echo
    echo "https://github.com/cmu-db/peloton/issues"
    echo
    exit 1
fi
