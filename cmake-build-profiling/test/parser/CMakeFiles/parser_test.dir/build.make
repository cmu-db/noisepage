# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.14

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake

# The command to remove a file.
RM = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/vivianhuang/Desktop/terrier

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/vivianhuang/Desktop/terrier/cmake-build-profiling

# Include any dependencies generated for this target.
include test/parser/CMakeFiles/parser_test.dir/depend.make

# Include the progress variables for this target.
include test/parser/CMakeFiles/parser_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/parser/CMakeFiles/parser_test.dir/flags.make

test/parser/CMakeFiles/parser_test.dir/parser_test.cpp.o: test/parser/CMakeFiles/parser_test.dir/flags.make
test/parser/CMakeFiles/parser_test.dir/parser_test.cpp.o: ../test/parser/parser_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/vivianhuang/Desktop/terrier/cmake-build-profiling/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/parser/CMakeFiles/parser_test.dir/parser_test.cpp.o"
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-profiling/test/parser && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/parser_test.dir/parser_test.cpp.o -c /Users/vivianhuang/Desktop/terrier/test/parser/parser_test.cpp

test/parser/CMakeFiles/parser_test.dir/parser_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/parser_test.dir/parser_test.cpp.i"
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-profiling/test/parser && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/vivianhuang/Desktop/terrier/test/parser/parser_test.cpp > CMakeFiles/parser_test.dir/parser_test.cpp.i

test/parser/CMakeFiles/parser_test.dir/parser_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/parser_test.dir/parser_test.cpp.s"
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-profiling/test/parser && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/vivianhuang/Desktop/terrier/test/parser/parser_test.cpp -o CMakeFiles/parser_test.dir/parser_test.cpp.s

# Object files for target parser_test
parser_test_OBJECTS = \
"CMakeFiles/parser_test.dir/parser_test.cpp.o"

# External object files for target parser_test
parser_test_EXTERNAL_OBJECTS =

relwithdebinfo/parser_test: test/parser/CMakeFiles/parser_test.dir/parser_test.cpp.o
relwithdebinfo/parser_test: test/parser/CMakeFiles/parser_test.dir/build.make
relwithdebinfo/parser_test: relwithdebinfo/libtest_util.a
relwithdebinfo/parser_test: relwithdebinfo/libterrier.a
relwithdebinfo/parser_test: googletest_ep-prefix/src/googletest_ep/lib/libgtest.a
relwithdebinfo/parser_test: googletest_ep-prefix/src/googletest_ep/lib/libgtest_main.a
relwithdebinfo/parser_test: relwithdebinfo/libpg_query.a
relwithdebinfo/parser_test: /usr/local/lib/libevent.dylib
relwithdebinfo/parser_test: /usr/local/lib/libevent_pthreads.dylib
relwithdebinfo/parser_test: /usr/local/lib/libtbb.dylib
relwithdebinfo/parser_test: /usr/local/lib/libpqxx.dylib
relwithdebinfo/parser_test: /usr/local/lib/libpq.dylib
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMMCJIT.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMExecutionEngine.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMRuntimeDyld.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86CodeGen.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMAsmPrinter.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMDebugInfoCodeView.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMDebugInfoMSF.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMGlobalISel.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMSelectionDAG.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMCodeGen.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMTarget.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMBitWriter.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMScalarOpts.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMInstCombine.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMTransformUtils.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMAnalysis.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMProfileData.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86AsmParser.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86Desc.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMObject.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMBitReader.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMMCParser.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86AsmPrinter.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86Disassembler.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86Info.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMMCDisassembler.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMMC.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86Utils.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMCore.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMBinaryFormat.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMSupport.a
relwithdebinfo/parser_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMDemangle.a
relwithdebinfo/parser_test: /usr/lib/libsqlite3.dylib
relwithdebinfo/parser_test: gflags_ep-prefix/src/gflags_ep/lib/libgflags.a
relwithdebinfo/parser_test: /usr/lib/libpthread.dylib
relwithdebinfo/parser_test: test/parser/CMakeFiles/parser_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/vivianhuang/Desktop/terrier/cmake-build-profiling/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ../../relwithdebinfo/parser_test"
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-profiling/test/parser && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/parser_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/parser/CMakeFiles/parser_test.dir/build: relwithdebinfo/parser_test

.PHONY : test/parser/CMakeFiles/parser_test.dir/build

test/parser/CMakeFiles/parser_test.dir/clean:
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-profiling/test/parser && $(CMAKE_COMMAND) -P CMakeFiles/parser_test.dir/cmake_clean.cmake
.PHONY : test/parser/CMakeFiles/parser_test.dir/clean

test/parser/CMakeFiles/parser_test.dir/depend:
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-profiling && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/vivianhuang/Desktop/terrier /Users/vivianhuang/Desktop/terrier/test/parser /Users/vivianhuang/Desktop/terrier/cmake-build-profiling /Users/vivianhuang/Desktop/terrier/cmake-build-profiling/test/parser /Users/vivianhuang/Desktop/terrier/cmake-build-profiling/test/parser/CMakeFiles/parser_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/parser/CMakeFiles/parser_test.dir/depend

