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

# Source: https://github.com/apache/incubator-retired-quickstep/blob/master/cmake/FindLibNuma.cmake

# Module to find LibNuma
find_path(LIBNUMA_INCLUDE_DIR NAMES numa.h numaif.h)
find_library(LIBNUMA_LIBRARY NAMES numa)

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibNuma DEFAULT_MSG
        LIBNUMA_LIBRARY
        LIBNUMA_INCLUDE_DIR)

mark_as_advanced(LIBNUMA_INCLUDE_DIR LIBNUMA_LIBRARY)
