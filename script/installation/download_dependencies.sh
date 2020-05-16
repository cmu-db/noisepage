#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Modified from the Apache Arrow project for the Terrier project.

# This script downloads all the thirdparty dependencies as a series of tarballs
# that can be used for offline builds, etc.

set -e

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../../third_party" && pwd )"


if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <destination-directory>"
  exit
fi

_DST=$1

# To change toolchain versions, edit versions.txt
source $SOURCE_DIR/versions.txt

mkdir -p $_DST

wget -c -O $_DST/gtest.tar.gz https://github.com/google/googletest/archive/release-$GTEST_VERSION.tar.gz

wget -c -O $_DST/gflags.tar.gz https://github.com/gflags/gflags/archive/v$GFLAGS_VERSION.tar.gz

wget -c -O $_DST/gbenchmark.tar.gz https://github.com/google/benchmark/archive/v$GBENCHMARK_VERSION.tar.gz

echo "
# Environment variables for offline Terrier build
export TERRIER_GTEST_URL=$_DST/gtest.tar.gz
export TERRIER_GFLAGS_URL=$_DST/gflags.tar.gz
export TERRIER_GBENCHMARK_URL=$_DST/gbenchmark.tar.gz
"
