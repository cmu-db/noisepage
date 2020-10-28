#!/usr/bin/env python3
"""
Usage: tpl_bytecode_handlers_ir_compiler.py LLVM_COMPILER CMAKE_BINARY_DIR BCH_CPP BCH_OUT
    where
        LLVM_COMPILER       =   The path to clang++ on the system.
        CMAKE_BINARY_DIR    =   The build directory, which must contain compile_commands.json.
        BCH_CPP             =   The path to the bytecode_handlers_ip.cpp file to be compiled.
        BCH_OUT             =   The output path for the compiled file.

Compiles bytecode_handlers_ir.cpp with clang++.
This script is necessary because it is not possible to mix compilers in a single CMake project
without going through some superproject / externalproject shenanigans. Moreover, with "modern"
target-based CMake, you do not have access to a convenient list of all the CMAKE_CXX_FLAGS
that another compiler should compile with, and you do not have an easy way of extracting a
full set of target properties for compilation.

So instead, we parse compile_commands.json to get the flags that bytecode_handlers_ir.cpp
should be compiled with. Note that we depend on the command being
    /usr/bin/c++ (WE EXTRACT THIS) -o blah -c blah
which is the case for at least CMake 3.16 on Ubuntu when generating for both Make and Ninja.

Fortunately, we are only compiling with common flags which are shared by both gcc and clang.
If this changes, we may need the above superproject / externalproject solutions.
"""

import os
import subprocess
import sys

PROGRAM_NAME = sys.argv[0]
# The Clang compiler that will emit LLVM.
PATH_TO_LLVM_COMPILER = sys.argv[1]
# cd to the build directory, which should have a compile_commands.json file.
PATH_TO_CMAKE_BINARY_DIR = sys.argv[2]
# The bytecode_handlers_ir.cpp file to be compiled.
PATH_TO_BCH_CPP = sys.argv[3]
# The output path and filename.
PATH_TO_BCH_OUT = sys.argv[4]


def GetClangFlags():
    prev = ''
    with open('compile_commands.json') as f:
        for line in f:
            # Look for the line that ends with tpl.cpp". The preceding line should be the compilation command.
            if line.endswith('bytecode_handlers_ir.cpp"\n'):
                command = prev
                # Some magic parsing logic. I hate this.
                _, _, _, command, _ = command.split('"')
                # Remove the compiler (idx 0) and executable (-o blah -c blahblah).
                command = command.split(' ')[1:-4]
                # Return the compile command.
                return [x for x in command if x != '' and x != '--coverage' and x != '-fPIC']
            # Record the line for the next iteration.
            prev = line
    raise Exception("Could not find bytecode_handlers_ir.cpp in compile_commands.json.")


if __name__ == '__main__':
    os.chdir(PATH_TO_CMAKE_BINARY_DIR)
    call = [PATH_TO_LLVM_COMPILER] + GetClangFlags() + ["-emit-llvm", "-o", PATH_TO_BCH_OUT, "-c", PATH_TO_BCH_CPP]
    call = ' '.join(call)
    try:
        subprocess.check_call(call, shell=True)
        print('{} invoked: {}'.format(PROGRAM_NAME, call))
    except:
        sys.exit(-1)
