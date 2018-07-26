FROM ubuntu:18.04
CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -y update && \
    apt-get -y install apt-utils && \
    apt-get -y install \
      autoconf \
      automake \
      curl \
      clang-6.0 \
      clang-format \
      clang-tidy \
      cmake \
      doxygen \
      gcc-7 \
      g++-7 \
      git \
      graphviz \
      lcov \
      libgflags-dev \
      libboost-dev \
      libboost-filesystem-dev \
      libboost-thread-dev \
      libjemalloc-dev \
      libjsoncpp-dev \
      libtbb-dev \
      libtool \
      libz-dev \
      llvm-6.0 \
      make \
      python3-pip \
      valgrind
