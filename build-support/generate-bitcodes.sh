#!/bin/bash
# generate-bitcodes.sh
#
# Generate bytecode handlers bitcode file, if necessary.
# If the file is already present in the provided directory,
# this script is a no-op.

# TODO(Kyle): above we assume bash is an available shell, should we be doing this?

# The root path of the project (i.e. noisepage/)
PROJECT_ROOT=$1
# The CMake artifact directory (i.e. noisepage/build)
BINARY_DIR=$2
# The path to clang
CLANG_PATH=$3

# Determine if the file is already present
if [ -f "$BINARY_DIR/bin/bytecode_handlers_ir.bc" ]; then
  exit 0
fi

# Run the compiler
${PROJECT_ROOT}/build-support/tpl_bytecode_handlers_ir_compiler.py ${CLANG_PATH} ${BINARY_DIR} ${PROJECT_ROOT}/util/execution/bytecode_handlers_ir.cpp ${BINARY_DIR}/bin/bytecode_handlers_ir.bc || exit 1

# Optimize
${BINARY_DIR}/bin/gen_opt_bc ${BINARY_DIR}/bin/bytecode_handlers_ir.bc ${BINARY_DIR}/bin/bytecode_handlers_opt.bc || exit 1

# Move the generated file to its final location
mv ${BINARY_DIR}/bin/bytecode_handlers_opt.bc ${BINARY_DIR}/bin/bytecode_handlers_ir.bc || exit 1

exit 0