#!/usr/bin/env python3
#
# tpl_bytecode_handlers_ir_compiler.py
# Compiles bytecode_handlers_ir.cpp with clang++.
#
# Usage: 
#
#   tpl_bytecode_handlers_ir_compiler.py LLVM_COMPILER CMAKE_BINARY_DIR BCH_CPP BCH_OUT
#    
#   where
#         LLVM_COMPILER       =   The path to clang++ on the system.
#         CMAKE_BINARY_DIR    =   The build directory, which must contain compile_commands.json.
#         BCH_CPP             =   The path to the bytecode_handlers_ip.cpp file to be compiled.
#         BCH_OUT             =   The output path for the compiled file.
#
#
# This script is necessary because it is not possible to mix compilers in a single CMake project
# without going through some superproject / externalproject shenanigans. Moreover, with "modern"
# target-based CMake, you do not have access to a convenient list of all the CMAKE_CXX_FLAGS
# that another compiler should compile with, and you do not have an easy way of extracting a
# full set of target properties for compilation.
#
# So instead, we parse compile_commands.json to get the flags that bytecode_handlers_ir.cpp
# should be compiled with. Note that we depend on the command being
#     /usr/bin/c++ (WE EXTRACT THIS) -o blah -c blah
# which is the case for at least CMake 3.16 on Ubuntu when generating for both Make and Ninja.
#
# Fortunately, we are only compiling with common flags which are shared by both gcc and clang.
# If this changes, we may need the above superproject / externalproject solutions.

import os
import sys
import subprocess
from typing import List

# Script exit codes
EXIT_SUCCESS =  0
EXIT_FAILURE = -1

PROGRAM_NAME = sys.argv[0]

# The Clang compiler that will emit LLVM.
PATH_TO_LLVM_COMPILER = sys.argv[1]

# cd to the build directory, which should have a compile_commands.json file.
PATH_TO_CMAKE_BINARY_DIR = sys.argv[2]

# The bytecode_handlers_ir.cpp file to be compiled.
PATH_TO_BCH_CPP = sys.argv[3]

# The output path and filename.
PATH_TO_BCH_OUT = sys.argv[4]

# Those flags that we do not want passed through to
# clang++ for compilation of the bytecode handlers
FLAG_BLACKLIST = [
    "",
    "--coverage",          # Relevant?
    "-fPIC",               # Relevant?
    "-ggdb",               # No need for debug symbols
    "-fsanitize=address"   # Don't want ASAN instrumentation
]

# Those flags that we want to transform in some way
# as they are 
FLAG_TRANSFORMS = {
    "-O0": "-O3",  # Always optimize
}

def apply_transform(flag: str) -> str:
    """
    Apply any transformations defined for the flag if they
    are present; otherwise return the flag unmodified.
    :param flag The input flag
    :return The flag with transformation applied
    """
    return FLAG_TRANSFORMS[flag] if flag in FLAG_TRANSFORMS else flag

def get_clang_flags() -> List[str]:
    """
    Compute the flags passed to clang++ to compile the bytecodes.
    :return A list of the flags to pass to clang++ (strings)
    """
    prev = ""
    with open("compile_commands.json") as f:
        for line in f:
            # Look for the line that ends with bytecode_handlers_ir.cpp".
            # The preceding line should be the compilation command.
            if line.endswith('bytecode_handlers_ir.cpp"\n'):
                command = prev
                # Some magic parsing logic. I hate this.
                _, _, _, command, _ = command.split('"')
                # Remove the compiler (idx 0) and executable (-o blah -c blahblah).
                command = command.split(' ')[1:-4]
                # Return the compile command.
                return [apply_transform(c) for c in filter(lambda x: x not in FLAG_BLACKLIST, command)]
            
            # Record the line for the next iteration.
            prev = line
    raise Exception("Could not find bytecode_handlers_ir.cpp in compile_commands.json.")

def main() -> int:
    os.chdir(PATH_TO_CMAKE_BINARY_DIR)
    call = [PATH_TO_LLVM_COMPILER] + get_clang_flags() + ["-emit-llvm", "-o", PATH_TO_BCH_OUT, "-c", PATH_TO_BCH_CPP]
    call = " ".join(call)
    try:
        subprocess.check_call(call, shell=True)
        print("{} invoked: {}".format(PROGRAM_NAME, call))
    except:
        return EXIT_FAILURE

    return EXIT_SUCCESS

if __name__ == "__main__":
    sys.exit(main())
