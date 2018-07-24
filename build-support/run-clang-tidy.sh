#!/bin/bash
#
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
#
#
# Runs clang format in the given directory
# Arguments:
#   $1 - Path to the clang tidy binary
#   $2 - Path to the compile_commands.json to use
#   $3 - Apply fixes (will raise an error if false and not there where changes)
#   $ARGN - Files to run clang-tidy on
#
CLANG_TIDY=$1
shift
COMPILE_COMMANDS=$1
shift
APPLY_FIXES=$1
shift

# clang format will only find its configuration if we are in
# the source tree or in a path relative to the source tree
if [ "$APPLY_FIXES" == "1" ]; then
  $CLANG_TIDY -p $COMPILE_COMMANDS -fix  $@
else
  $CLANG_TIDY -p $COMPILE_COMMANDS  $@
  NUM_CORRECTIONS=`$CLANG_TIDY -p $COMPILE_COMMANDS $@ 2>&1 | grep -v Skipping | grep "warnings* generated" | wc -l`
  if [ "$NUM_CORRECTIONS" -gt "0" ]; then
    echo "clang-tidy had suggested fixes.  Please fix these!!!"
    exit 1
  fi
fi
