FROM ubuntu:18.04
CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get -y update && \
    apt-get -y install \
      build-essential \
      clang-7 \
      clang-format-7 \
      clang-tidy-7 \
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
      llvm-7 \
      pkg-config \
      postgresql-client \
      sqlite3 \
      libsqlite3-dev \
      ant && \
   apt-get -y install wget && \
   wget http://mirrors.kernel.org/ubuntu/pool/universe/libp/libpqxx/libpqxx-dev_6.2.4-4_amd64.deb && \
   wget http://mirrors.kernel.org/ubuntu/pool/universe/libp/libpqxx/libpqxx-6.2_6.2.4-4_amd64.deb && \
   dpkg -i libpqxx-6.2_6.2.4-4_amd64.deb && \
   dpkg -i libpqxx-dev_6.2.4-4_amd64.deb
