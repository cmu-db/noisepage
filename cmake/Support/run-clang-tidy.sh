#!/bin/bash
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
fi
