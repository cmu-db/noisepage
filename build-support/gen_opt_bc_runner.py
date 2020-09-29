#!/usr/bin/env python
import os
import subprocess
import sys


# The Clang compiler that will emit LLVM.
PATH_TO_LLVM_COMPILER = sys.argv[1]
# cd to the build directory, which should have a compile_commands.json file.
PATH_TO_CMAKE_BINARY_DIR = sys.argv[2]
os.chdir(PATH_TO_CMAKE_BINARY_DIR)
# The bytecode_handlers_ir.cpp file to be compiled.
PATH_TO_BCH_CPP = sys.argv[3]
# The output path and filename.
PATH_TO_BCH_OUT = sys.argv[4]

def GetClangFlags():
    prev = ''
    with open('compile_commands.json') as f:
        for line in f:
            # Look for the line that ends with tpl.cpp". The preceding line should be the compilation command.
            if line.endswith('tpl.cpp"\n'):
                command = prev
                # Some magic parsing logic. I hate this.
                _, _, _, command, _ = command.split('"')
                # Remove the compiler (idx 0) and executable (-o blah -c blahblah).
                command = command.split(' ')[1:-4]
                # Return the compile command.
                return [x for x in command if x != '']
            # Record the line for the next iteration.
            prev = line
    raise Exception("Could not find tpl.cpp in compile_commands.json.")


if __name__ == '__main__':
    call = [PATH_TO_LLVM_COMPILER] + GetClangFlags() + ["-emit-llvm", "-o", PATH_TO_BCH_OUT, "-c", PATH_TO_BCH_CPP]
    call = ' '.join(call)
    try:
        print(call)
        subprocess.check_call(call, shell=True)
        sys.exit(0)
    except:
        sys.exit(-1)