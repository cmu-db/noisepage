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
CMAKE_BINARY_DIR = /Users/vivianhuang/Desktop/terrier/cmake-build-development

# Include any dependencies generated for this target.
include test/traffic_cop/CMakeFiles/traffic_cop_test.dir/depend.make

# Include the progress variables for this target.
include test/traffic_cop/CMakeFiles/traffic_cop_test.dir/progress.make

# Include the compile flags for this target's objects.
include test/traffic_cop/CMakeFiles/traffic_cop_test.dir/flags.make

test/traffic_cop/CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.o: test/traffic_cop/CMakeFiles/traffic_cop_test.dir/flags.make
test/traffic_cop/CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.o: ../test/traffic_cop/traffic_cop_test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/vivianhuang/Desktop/terrier/cmake-build-development/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object test/traffic_cop/CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.o"
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-development/test/traffic_cop && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.o -c /Users/vivianhuang/Desktop/terrier/test/traffic_cop/traffic_cop_test.cpp

test/traffic_cop/CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.i"
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-development/test/traffic_cop && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/vivianhuang/Desktop/terrier/test/traffic_cop/traffic_cop_test.cpp > CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.i

test/traffic_cop/CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.s"
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-development/test/traffic_cop && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/vivianhuang/Desktop/terrier/test/traffic_cop/traffic_cop_test.cpp -o CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.s

# Object files for target traffic_cop_test
traffic_cop_test_OBJECTS = \
"CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.o"

# External object files for target traffic_cop_test
traffic_cop_test_EXTERNAL_OBJECTS =

debug/traffic_cop_test: test/traffic_cop/CMakeFiles/traffic_cop_test.dir/traffic_cop_test.cpp.o
debug/traffic_cop_test: test/traffic_cop/CMakeFiles/traffic_cop_test.dir/build.make
debug/traffic_cop_test: debug/libtest_util.a
debug/traffic_cop_test: debug/libterrier.a
debug/traffic_cop_test: googletest_ep-prefix/src/googletest_ep/lib/libgtest.a
debug/traffic_cop_test: googletest_ep-prefix/src/googletest_ep/lib/libgtest_main.a
debug/traffic_cop_test: debug/libpg_query.a
debug/traffic_cop_test: /usr/local/lib/libevent.dylib
debug/traffic_cop_test: /usr/local/lib/libevent_pthreads.dylib
debug/traffic_cop_test: /usr/local/lib/libtbb.dylib
debug/traffic_cop_test: /usr/local/lib/libpqxx.dylib
debug/traffic_cop_test: /usr/local/lib/libpq.dylib
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMMCJIT.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMExecutionEngine.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMRuntimeDyld.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86CodeGen.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMAsmPrinter.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMDebugInfoCodeView.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMDebugInfoMSF.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMGlobalISel.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMSelectionDAG.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMCodeGen.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMTarget.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMBitWriter.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMScalarOpts.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMInstCombine.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMTransformUtils.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMAnalysis.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMProfileData.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86AsmParser.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86Desc.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMObject.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMBitReader.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMMCParser.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86AsmPrinter.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86Disassembler.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86Info.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMMCDisassembler.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMMC.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMX86Utils.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMCore.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMBinaryFormat.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMSupport.a
debug/traffic_cop_test: /usr/local/Cellar/llvm@6/6.0.1_1/lib/libLLVMDemangle.a
debug/traffic_cop_test: /usr/lib/libsqlite3.dylib
debug/traffic_cop_test: gflags_ep-prefix/src/gflags_ep/lib/libgflags.a
debug/traffic_cop_test: /usr/lib/libpthread.dylib
debug/traffic_cop_test: test/traffic_cop/CMakeFiles/traffic_cop_test.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/vivianhuang/Desktop/terrier/cmake-build-development/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable ../../debug/traffic_cop_test"
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-development/test/traffic_cop && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/traffic_cop_test.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
test/traffic_cop/CMakeFiles/traffic_cop_test.dir/build: debug/traffic_cop_test

.PHONY : test/traffic_cop/CMakeFiles/traffic_cop_test.dir/build

test/traffic_cop/CMakeFiles/traffic_cop_test.dir/clean:
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-development/test/traffic_cop && $(CMAKE_COMMAND) -P CMakeFiles/traffic_cop_test.dir/cmake_clean.cmake
.PHONY : test/traffic_cop/CMakeFiles/traffic_cop_test.dir/clean

test/traffic_cop/CMakeFiles/traffic_cop_test.dir/depend:
	cd /Users/vivianhuang/Desktop/terrier/cmake-build-development && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/vivianhuang/Desktop/terrier /Users/vivianhuang/Desktop/terrier/test/traffic_cop /Users/vivianhuang/Desktop/terrier/cmake-build-development /Users/vivianhuang/Desktop/terrier/cmake-build-development/test/traffic_cop /Users/vivianhuang/Desktop/terrier/cmake-build-development/test/traffic_cop/CMakeFiles/traffic_cop_test.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : test/traffic_cop/CMakeFiles/traffic_cop_test.dir/depend

