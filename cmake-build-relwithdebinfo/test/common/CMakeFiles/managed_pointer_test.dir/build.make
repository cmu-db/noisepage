# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.15

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
CMAKE_SOURCE_DIR = /Users/dpatra/Research/terrier

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo

# Include any dependencies generated for this target.
include test/common/CMakeFiles/managed_pointer_test.dir/depend.make

# Include the progress variables for this target.
include test/common/CMakeFiles/managed_pointer_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/common/CMakeFiles/managed_pointer_test.dir/flags.make

test/common/CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.o: test/common/CMakeFiles/managed_pointer_test.dir/flags.make
test/common/CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.o: ../test/common/managed_pointer_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/common/CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.o"
	cd /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/test/common && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.o -c /Users/dpatra/Research/terrier/test/common/managed_pointer_test.cpp

test/common/CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.i"
	cd /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/test/common && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/dpatra/Research/terrier/test/common/managed_pointer_test.cpp > CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.i

test/common/CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.s"
	cd /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/test/common && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/dpatra/Research/terrier/test/common/managed_pointer_test.cpp -o CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.s

# Object files for target managed_pointer_test
managed_pointer_test_OBJECTS = \
"CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.o"

# External object files for target managed_pointer_test
managed_pointer_test_EXTERNAL_OBJECTS =

relwithdebinfo/managed_pointer_test: test/common/CMakeFiles/managed_pointer_test.dir/managed_pointer_test.cpp.o
relwithdebinfo/managed_pointer_test: test/common/CMakeFiles/managed_pointer_test.dir/build.make
relwithdebinfo/managed_pointer_test: relwithdebinfo/libtest_util.a
relwithdebinfo/managed_pointer_test: relwithdebinfo/libterrier.a
relwithdebinfo/managed_pointer_test: googletest_ep-prefix/src/googletest_ep/lib/libgtest.a
relwithdebinfo/managed_pointer_test: googletest_ep-prefix/src/googletest_ep/lib/libgmock_main.a
relwithdebinfo/managed_pointer_test: gflags_ep-prefix/src/gflags_ep/lib/libgflags.a
relwithdebinfo/managed_pointer_test: relwithdebinfo/libutil_static.a
relwithdebinfo/managed_pointer_test: relwithdebinfo/libterrier.a
relwithdebinfo/managed_pointer_test: gflags_ep-prefix/src/gflags_ep/lib/libgflags.a
relwithdebinfo/managed_pointer_test: /usr/local/lib/libevent.dylib
relwithdebinfo/managed_pointer_test: /usr/local/lib/libevent_pthreads.dylib
relwithdebinfo/managed_pointer_test: /usr/local/lib/libtbb.dylib
relwithdebinfo/managed_pointer_test: /usr/local/lib/libpqxx.dylib
relwithdebinfo/managed_pointer_test: /usr/local/lib/libpq.dylib
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMMCJIT.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMExecutionEngine.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMRuntimeDyld.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMX86CodeGen.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMAsmPrinter.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMGlobalISel.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMSelectionDAG.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMCodeGen.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMTarget.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMipo.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMBitWriter.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMIRReader.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMAsmParser.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMInstrumentation.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMLinker.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMScalarOpts.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMAggressiveInstCombine.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMInstCombine.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMVectorize.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMTransformUtils.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMAnalysis.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMProfileData.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMX86AsmParser.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMX86Desc.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMObject.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMBitReader.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMCore.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMMCParser.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMX86AsmPrinter.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMX86Disassembler.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMX86Info.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMMCDisassembler.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMMC.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMBinaryFormat.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMDebugInfoCodeView.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMDebugInfoMSF.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMX86Utils.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMSupport.a
relwithdebinfo/managed_pointer_test: /usr/local/Cellar/llvm@8/8.0.1_1/lib/libLLVMDemangle.a
relwithdebinfo/managed_pointer_test: /usr/lib/libsqlite3.dylib
relwithdebinfo/managed_pointer_test: relwithdebinfo/libpg_query.a
relwithdebinfo/managed_pointer_test: test/common/CMakeFiles/managed_pointer_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ../../relwithdebinfo/managed_pointer_test"
	cd /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/test/common && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/managed_pointer_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/common/CMakeFiles/managed_pointer_test.dir/build: relwithdebinfo/managed_pointer_test

.PHONY : test/common/CMakeFiles/managed_pointer_test.dir/build

test/common/CMakeFiles/managed_pointer_test.dir/clean:
	cd /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/test/common && $(CMAKE_COMMAND) -P CMakeFiles/managed_pointer_test.dir/cmake_clean.cmake
.PHONY : test/common/CMakeFiles/managed_pointer_test.dir/clean

test/common/CMakeFiles/managed_pointer_test.dir/depend:
	cd /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/dpatra/Research/terrier /Users/dpatra/Research/terrier/test/common /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/test/common /Users/dpatra/Research/terrier/cmake-build-relwithdebinfo/test/common/CMakeFiles/managed_pointer_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/common/CMakeFiles/managed_pointer_test.dir/depend

