# C++ Guidelines

**Table of Contents**

- [Language](#language)
  - [C++ Crash Course](#c++-crash-course)
  - [C++ Project Structure](#c++-project-structure)
  - [C++ Code Style](#c++-code-style)
- [Compiler](#compiler)
- [Debugging](#debugging)
- [Testing](#testing)
- [Profiling](#profiling)
- [Documentation](#documentation)

## Language

NoisePage is developed in `C++17`.

### C++ Crash Course

C++ provides a lot of leeway in DBMS development compared to other high-level languages. For instance, it supports both manual and automated memory management, varied styles of programming, stronger type checking, different kinds of polymorphism etc. 

Here's a list of useful references : 

* [cppreference](http://en.cppreference.com/w/cpp) is an online reference of the powerful `Standard Template Library` (STL).
* [C++ FAQ](https://isocpp.org/faq) covers a lots of topics.

Here's a list of modern features that you might want to make use of:

* `auto` type inference
* Range-based `for` loops
* Smart pointers, in particular `unique_ptr`.
* STL data structures, such as `unordered_set`, `unordered_map`, etc.
* Threads, deleted member functions, lambdas, etc.

### C++ Project Structure
 
#### Directory Structure

Organize source code files into relevant folders based on their functionality. Separate binary files from source files, and production code from testing code. 

**Code directories**
  * `src`: This is where the bulk of the code for NoisePage lives. Anything that you expect to be compiled into the release should be here.
  * `benchmark`: This is where Google Benchmarks and their utility code reside. `src` should not have dependencies going into `benchmark`.
  * `test`: This is where Google Tests and their utility code reside. `src` should not have dependencies going into `test`. `benchmark` may have dependencies going into `test`.
  * `third_party`: This is where we add code which was not written by us but which we need to modify. If you want to bring in a new third party dependency unmodified, look for `add_noisepage_dep` examples in `CMakeLists.txt`.

**Infrastructure directories**
  * `script`: Scripts that support development and testing lives here. (e.g. dependency installation).
  * `apidoc`: Doxygen-related files. Build documentation by executing `doxygen Doxyfile.in` in this directory.
  * `build-support`: Files that support Continuous Integration CMake targets (lint, format, etc.).
  * `cmake_modules`: CMake scripts for the build infrastructure.

You will almost never need to create new directories, ask on Slack before doing so. Here are guidelines for adding files to each existing directory.

##### src
There can be at most 2-levels of directories under `src`, the first level will be general system components (e.g. storage, execution, network, sql, common), and the second level will be either for a class of similar files, or for a self-contained sub-component.

Translated into coding guidelines, you should rarely need to create a new first-level subdirectory, and should probably ask on Slack if you believe you do. To create a new secondary directory, make sure you meet the following criteria:
  * There are more than 2 (exclusive) files you need to put into this folder
  * Each file is stand-alone, i.e. either the contents don't make sense living in a single file, or that putting them in a single file makes the file large and difficult to navigate. (This is open to interpretation, but if, for example, you have 3 files containing 10-line class definitions, maybe they should not be spread out that much).

And one of the two:
  * The subdirectory is a self-contained sub-component. This probably means that the folder only has one outward facing API. A good rule of thumb is when outside code files only need to include one header from this folder, where said API is defined.
  * The subdirectory contains a logical grouping of files, and there are enough of them that leaving them ungrouped makes the upper level hard to navigate. (e.g. all the plans, all the common data structures, etc.)

A good rule of thumb is if you have subdirectory `As`, you should be able to say with a straight face that everything under `As` is an A. (e.g. Everything under `containers` is a container)

Every class and/or function under these directories should be in namespaces. All code will be under namespace `noisepage`, and namespace the same as their first-level directory name (e.g `common`, `storage`). Secondary sub-directories do not have associated namespaces.

##### test
The directory structure of the `test` folder should generally reflect the directory structure of the `src` folder, ignoring the `include`. Each test should be under the same path as the file they test, and named "XXX_test" (for example, `src/include/common/container/bitmap.h` will be tested under `test/common/container/bitmap_test.cpp`)

Generally, there can be no code sharing between tests since they are different build targets. There are cases, however, where it makes sense for tests to share some common utility function. In that case, you can write a utility file under `test/util`.

The `test/util` folder should have no sub-directories. In most cases, one test util file for every directory under `src` should suffice. (e.g. `test/util/storage_test_util.h`) Sometimes it will make sense to have a `util` file for stand-alone modules (e.g. `test/include/util/random_test_util.h`).

### C++ Code Style

See [here](https://github.com/cmu-db/noisepage/tree/master/docs/cpp_guidelines_code_style.md).

## Compiler

We support GCC and LLVM Clang. **We do NOT support AppleClang** aka whatever compiler comes on macOS by default.

How is the compiler actually invoked?

1. CMake is a *build system generator* that is commonly used for C++ development.
   - CMake does not compile your program.
   - Running `cmake <PATH TO FOLDER WITH CMakeLists.txt> <OPTIONAL ARGUMENTS>` generates a system that will compile your program.
2. CMake uses either [make](https://en.wikipedia.org/wiki/Make_(software)) (the default) or [ninja](https://ninja-build.org/) (requested by passing `-GNinja` as an argument to `cmake`).
   - We strongly encourage using `ninja`, which is faster and can intelligently build in parallel by default.

For example, to manually compile NoisePage, this is what you would do:

1. Clone the NoisePage repo: `git clone https://github.com/cmu-db/noisepage.git`
2. Create a build folder to build everything in: `mkdir build`
3. Go to the build folder: `cd build`
4. Generate a build system with CMake, passing in whatever arguments are desired: `cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DNOISEPAGE_USE_JEMALLOC=ON -DNOISEPAGE_UNITY_BUILD=ON`
   - This is the Build / hammer button in CLion.
5. Invoke the build system: `ninja noisepage`, more generally `ninja <NAME OF TARGET>` for any valid target.
   - This is the Run / play button in CLion.

We have configured the build system so that it will produce a `compile_commands.json` file. This contains the exact compiler invocations that will be used. You can check this file if you're curious. This file is also used by tools like `clang-tidy` to check for correctness. If you are not sure what a compiler flag does, look it up on Google or on the `man` page for `gcc` or `clang`. 

## Debugging

You should use a debugger to find any bugs where possible. CLion's built-in debugger is [fantastic](https://blog.jetbrains.com/clion/2015/05/debug-clion/).

If you need to do complex debugging, you may want to check out the following links:

- [gdb](https://www.gnu.org/software/gdb/): General GDB documentation. CLion uses this by default.
- [lldb](https://lldb.llvm.org/): General LLDB documentation. A competitor to GDB.
- [rr](https://rr-project.org/): A reversible debugger that lets you go backwards in time. Very powerful, but requires some level of hardware support.

## Testing

Unit tests are critical for ensuring the correct functionality of your modules and reduce time spent on debugging. It can help prevent regressions. We use [googletest](https://github.com/google/googletest), a nice unit-testing framework for C++ projects. 

You should write unit test cases for each class/algorithm that you have added or modified. See the testing section for detail. Try to come up with test cases that make sure that the module exhibits the desired behavior. Some developers even suggest writing the unit tests before implementing the code. Make sure that you include corner cases, and try to find off-by-one errors. 

## Profiling

Profilers help better understand the performance of NoisePage and the environment in which it is running. We suggest `perf` and `strace` tools for profiling. CLion also has a built-in [profiler](https://www.jetbrains.com/help/clion/cpu-profiler.html) now.
