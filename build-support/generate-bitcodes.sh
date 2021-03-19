#!/bin/bash
# generate-bitcodes.sh
#
# Generate bytecode handlers bitcode file.

# The root path of the project (i.e. noisepage/)
PROJECT_ROOT=$1
# The CMake artifact directory (i.e. noisepage/build)
BINARY_DIR=$2
# The path to clang
CLANG_PATH=$3

echo "Generating optimized bitcodes..."

# Run the compiler
echo "Running: ${PROJECT_ROOT}/build-support/tpl_bytecode_handlers_ir_compiler.py ${CLANG_PATH} ${BINARY_DIR} ${PROJECT_ROOT}/util/execution/bytecode_handlers_ir.cpp ${BINARY_DIR}/bin/bytecode_handlers_ir.bc"
${PROJECT_ROOT}/build-support/tpl_bytecode_handlers_ir_compiler.py ${CLANG_PATH} ${BINARY_DIR} ${PROJECT_ROOT}/util/execution/bytecode_handlers_ir.cpp ${BINARY_DIR}/bin/bytecode_handlers_ir.bc || exit 1

# Optimize
echo "Running: ${BINARY_DIR}/bin/gen_opt_bc ${BINARY_DIR}/bin/bytecode_handlers_ir.bc ${BINARY_DIR}/bin/bytecode_handlers_opt.bc"
${BINARY_DIR}/bin/gen_opt_bc ${BINARY_DIR}/bin/bytecode_handlers_ir.bc ${BINARY_DIR}/bin/bytecode_handlers_opt.bc || exit 1

# Move the generated file to its final location
echo "Running: mv ${BINARY_DIR}/bin/bytecode_handlers_opt.bc ${BINARY_DIR}/bin/bytecode_handlers_ir.bc"
mv ${BINARY_DIR}/bin/bytecode_handlers_opt.bc ${BINARY_DIR}/bin/bytecode_handlers_ir.bc || exit 1

# Success
exit 0