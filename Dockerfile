FROM ubuntu:18.04
CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive

WORKDIR /usr/include/

RUN apt-get -y update && \
    apt-get -y install \
      build-essential \
      clang-8 \
      clang-format-8 \
      clang-tidy-8 \
      cmake \
      doxygen \
      git \
      g++-7 \
      libboost-dev \
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
	  numactl \
	  libnuma-dev \
	  ccache && \
      apt-get -y install wget && \
      wget http://mirrors.kernel.org/ubuntu/pool/universe/libp/libpqxx/libpqxx-6.2_6.2.5-1_amd64.deb && \
      wget http://mirrors.kernel.org/ubuntu/pool/universe/libp/libpqxx/libpqxx-dev_6.2.5-1_amd64.deb && \
      dpkg -i libpqxx-6.2_6.2.5-1_amd64.deb && \
      dpkg -i libpqxx-dev_6.2.5-1_amd64.deb && \
      rm libpqxx-6.2_6.2.5-1_amd64.deb && \
      rm libpqxx-dev_6.2.5-1_amd64.deb # && \
#      wget https://dl.bintray.com/boostorg/release/1.72.0/source/boost_1_72_0.tar.gz && \
#      mkdir -p /usr/include/boost && \
#      tar zxf boost_1_72_0.tar.gz -C /usr/include/boost --strip-components=1 && \
#      rm boost_1_72_0.tar.gz \
#
#ENV BOOST_ROOT=/usr/include/boost
