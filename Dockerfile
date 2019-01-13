FROM ubuntu:18.04
CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -y update && \
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
      libjemalloc-dev \
      libevent-dev \
      libtbb-dev \
      libz-dev \
      llvm-6.0