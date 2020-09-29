# Resources for learning about _modern_ CMake:
# - https://llvm.org/docs/CMakePrimer.html LLVM's CMake guide. Start here.
# - https://www.youtube.com/watch?v=eC9-iRN2b04 Talk: CppCon2017 Mathieu Ropert "Using Modern CMake Patterns ...".
# - https://www.youtube.com/watch?v=bsXLMQ6WgIk Talk: C++Now 2017 Daniel Pfeifer "Effective CMake".
# - https://gist.github.com/mbinna/c61dbb39bca0e4fb7d1f73b0d66a4fd1 Gist: This is essentially Pfeifer's talk.
# - https://cliutils.gitlab.io/modern-cmake/    Book: Modern CMake. Some examples are no longer modern.
# - The official cmake.org site is full of outdated anti-patterns. Use for documentation, not for inspiration.

# Also, if you use CMake syntax for multiline comments, a kitten dies.
# If you didn't know that CMake has multiline comments, good.

# Explanations of various CMake quirks here.

#   1. A crash course on CMake's GLOB and GLOB_RECURSE.
#     CMake is not a build system -- CMake will not build your files.
#     CMake is a build system generator -- CMake will generate something (make, ninja, etc.) that can build your files.
#
#     How do you specify what files should get built?
#     - Well, you can either specify every single file manually, which is as horrible as it sounds. "Best practice".
#     - The old and not-recommended way is to use a GLOB, which creates a list of files.
#           Tangent: In CMake, a list is merely a ;-separated string. State of the art 2020 technology right there.
#
#     The problem with GLOB and GLOB_RECURSE is that the files are known to the build system _generator_, rather than
#     the build system. So you run GLOB and give a list of files to CMake. CMake hardcodes all these files when CMake
#     generates your build system. Much later, you add a new .h or .cpp file, and your build system doesn't pick it up,
#     because the build system needs to be regenerated! Incidentally, CLion has a File > Reload CMake Project button
#     handy for this reason. To summarize the problem: "if you're hardcoding a list of files into your build system,
#     you're not going to pick up new files".
#
#     Well, specifying every single source file in CMake still sounds like a pain. Instead, the band-aid hacky "modern"
#     solution is to specify CONFIGURE_DEPENDS. This means "if the build system supports this feature, if anything in
#     the GLOB changes, rerun CMake.
#
#   2. Footgun warning: target_include_directories
#       You must specify SYSTEM includes (typically disables warnings and errors from those includes) separately from
#       non-SYSTEM. In other words, always do
#           target_include_directories(target_name PUBLIC foo INTERFACE bar PRIVATE baz)
#           target_include_directories(target_name SYSTEM PUBLIC foo INTERFACE bar PRIVATE baz)
#       and never do
#           target_include_directories(target_name PUBLIC foo SYSTEM PRIVATE blah)
#       because the latter does not do what you think it does.


# Ubuntu 20.04 ships with CMake version 3.16.3.
cmake_minimum_required(VERSION 3.16)
# Create a compile_commands.json file that can be easily parsed.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# CMake has extremely limited support for multi-line string literals. Therefore an explicit CONCAT call is used here.
# This description of NoisePage is copied off the website.
string(
        CONCAT
        NOISEPAGE_DESCRIPTION
        "NoisePage is a relational DBMS designed from the ground up for autonomous deployment using machine learning "
        "to control its configuration, optimization, and tuning. Our research focuses on building system components "
        "that support such self-driving operation with little to no human guidance. We seek to create a system that "
        "is able not only to be able to optimize the system for the current workload but also to predict future "
        "workload trends and prepare itself accordingly. "
)

# Set the name of the CMake project to be noisepage. This also defines various magic variables, such as
# ${PROJECT_SOURCE_DIR} and ${PROJECT_BINARY_DIR}
project(
        noisepage
        # The version number of the NoisePage project.
        # The individual components can be extracted with ${PROJECT_VERSION_MAJOR}, ${PROJECT_VERSION_MINOR},
        # ${PROJECT_VERSION_PATCH}, and ${PROJECT_VERSION_TWEAK} respectively.
        VERSION 1.0.0.0
        DESCRIPTION "${NOISEPAGE_DESCRIPTION}"
        HOMEPAGE_URL "https://noise.page/"
        # NoisePage is a C++ project.
        LANGUAGES CXX
)

# People keep running CMake in the wrong folder, completely nuking their project or creating weird bugs.
# This checks if you're running CMake from a folder that already has CMakeLists.txt.
# Importantly, this catches the common case of running it from the root directory.
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" PATH_TO_CMAKELISTS_TXT)
if (EXISTS "${PATH_TO_CMAKELISTS_TXT}")
    message(FATAL_ERROR "Run CMake from a build subdirectory! \"mkdir build ; cd build ; cmake ..\" \
    Some junk files were created in this folder (CMakeCache.txt, CMakeFiles); you should delete those.")
endif ()

# Print a welcome message with the project's version number.
message(STATUS
        "Welcome to NoisePage!\n\
        Home Page: ${PROJECT_HOMEPAGE_URL}\n\
        Version: ${PROJECT_VERSION}\n")

# CMAKE_MODULE_PATH is the search path for the include() and find_package() CMake commands.
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")


message(STATUS "Compiler: ${CMAKE_CXX_COMPILER} ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

# Used for dependency finding
find_package(PkgConfig REQUIRED)

# Dependencies.
message(STATUS "\nDependencies: Locating...\n==========================================\n")

# This pair of VARS_BEFORE() and VARS_AFTER() macros helps you to figure out what variables external code has defined.
# For example, suppose you want to include the nlohmann_json library with
#       find_package(nlohmann_json REQUIRED).
# What variables did it define? Is there a version number in there? Who knows?
#       VARS_BEFORE()
#       find_package(nlohmann_json REQUIRED).
#       VARS_AFTER()
# And now you know! This is useful if you're adding new libraries and want to print useful messages.

macro(VARS_BEFORE)
    # Save the current variables to _vars_before.
    get_directory_property(_vars_before VARIABLES)
endmacro()

macro(VARS_AFTER)
    # Save the current variables to _vars_after.
    get_directory_property(_vars_after VARIABLES)
    # Diff _vars_after and _vars_before.
    list(REMOVE_ITEM _vars_after _vars_before ${_vars_before})
    # Print out all the variables that remain.
    foreach (_var IN LISTS _vars_after)
        message(STATUS "${_var} = ${${_var}}")
    endforeach ()
endmacro()

find_package(benchmark REQUIRED)
message(STATUS "[FOUND] benchmark (Google Benchmark) ${benchmark_VERSION}")

find_package(gflags REQUIRED)
message(STATUS "[FOUND] gflags (Google Commandline Flags) ${gflags_VERSION_STRING}")

find_package(GTest REQUIRED)
message(STATUS "[FOUND] GTest (Google Test) (No simple way to get version number)")

# nlohmann json : used for JSON serialization throughout the system.
find_package(nlohmann_json REQUIRED)
message(STATUS "[FOUND] nlohmann_json ${nlohmann_json_VERSION}")

# libpg_query.
add_subdirectory(
        ${PROJECT_SOURCE_DIR}/third_party/libpg_query/
        EXCLUDE_FROM_ALL
)
message(STATUS "[FOUND] libpq_query (DIY wrapper)")

add_subdirectory(
        ${PROJECT_SOURCE_DIR}/third_party/libcount/
        EXCLUDE_FROM_ALL
)
message(STATUS "[FOUND] libcount (DIY wrapper)")

pkg_search_module(EVENT REQUIRED libevent)
pkg_search_module(EVENT_PTHREADS REQUIRED libevent_pthreads)

# LLVM 8.0.
find_package(LLVM 8.0 REQUIRED CONFIG)
if (${LLVM_PACKAGE_VERSION} VERSION_LESS "8.0")
    message(FATAL_ERROR "LLVM 8.0 or newer is required.")
endif ()
message(STATUS "[FOUND] LLVM ${LLVM_PACKAGE_VERSION}")
# Explicitly request all of the LLVM components that we want.
llvm_map_components_to_libnames(LLVM_LIBRARIES core ipo mcjit nativecodegen native support)

# spdlog : ...

find_package(spdlog REQUIRED)
message(STATUS "[FOUND] TBB ${spdlog_VERSION}")

find_package(TBB)
message(STATUS "[FOUND] TBB ${TBB_VERSION}")

# xbyak : ...
add_subdirectory(
        ${PROJECT_SOURCE_DIR}/third_party/xbyak/                        # Include header-only xbyak.
        EXCLUDE_FROM_ALL                                                # Exclude from the ALL target.
)

# xxHash : ...
set(XXHASH_BUILD_XXHSUM OFF)                                            # Disable the xxHash command line binary.
add_subdirectory(
        ${PROJECT_SOURCE_DIR}/third_party/xxHash/cmake_unofficial/      # The directory specifying CMake magic.
        ${PROJECT_SOURCE_DIR}/third_party/xxHash/build/                 # The directory to build in.
        EXCLUDE_FROM_ALL                                                # Exclude from the ALL target.
)
get_target_property(XXHASH_VERSION xxhash VERSION)
message(STATUS "[FOUND] xxHash ${XXHASH_VERSION}")


message(STATUS "\n==========================================\nDependencies: All found!\n")

#######################################################################################################################
# noisepage_static      :   All of NoisePage functionality exposed as a static library.
#######################################################################################################################

file(GLOB_RECURSE
        NOISEPAGE_SRCS                  # Store the list of files into the variable ${NOISEPAGE_SRCS}.
        CONFIGURE_DEPENDS               # See above. Ask CMake to regenerate the build system if these files change.
        ${PROJECT_SOURCE_DIR}/src/*.cpp
        ${PROJECT_SOURCE_DIR}/src/include/*.h
        ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.cpp
        ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.h
        )
# Remove the main program from NoisePage sources.
list(REMOVE_ITEM NOISEPAGE_SRCS ${PROJECT_SOURCE_DIR}/src/main/terrier.cpp)

# Define the noisepage_static CMake target.
add_library(noisepage_static STATIC ${NOISEPAGE_SRCS})
target_compile_options(noisepage_static
        PRIVATE                                     # PRIVATE: only noisepage_static uses the following.
        "-Werror"                                   # Treat warnings as errors.
        "-Wall"                                     # Enable "all" warnings. (Not actually all warnings.)
        )
target_compile_options(noisepage_static
        PUBLIC                                      # PUBLIC: all consumers of noisepage_static inherit the following.
        "-march=native"                             # Enable machine-specific instruction sets and optimizations.
        "-mcx16"                                    # Allow CMPXCHG16B (16-byte compare and exchange).
        "-fsanitize=address"                        # Turn on Address Sanitizer.
        )
target_link_options(noisepage_static
        PUBLIC                                      # PUBLIC: all consumers of noisepage_static inherit the following.
        "-march=native"                             # Enable machine-specific instruction sets and optimizations.
        "-fsanitize=address"                        # Turn on Address Sanitizer.
        )
target_compile_features(noisepage_static PUBLIC
        cxx_std_17                                  # Require support for C++17.
        )
set_target_properties(noisepage_static PROPERTIES
        POSITION_INDEPENDENT_CODE ON                # Require position-independent code.
        CXX_EXTENSIONS OFF
        )
target_include_directories(noisepage_static PUBLIC
        ${PROJECT_SOURCE_DIR}/src/include
        )
target_include_directories(noisepage_static SYSTEM PUBLIC
        ${LLVM_INCLUDE_DIRS}
        )
target_link_libraries(noisepage_static PUBLIC
        gflags
        libcount::libcount
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        pg_query::pg_query
        xbyak::xbyak
        xxHash::xxhash
        ${EVENT_LINK_LIBRARIES}
        ${EVENT_PTHREADS_LINK_LIBRARIES}
        ${LLVM_LIBRARIES}
        ${TBB_LIBRARIES} # TBB_LIBRARIES_RELEASE
        )

get_target_property(FOO noisepage_static INCLUDE_DIRECTORIES)
message(STATUS "foo ${FOO}")

#######################################################################################################################
# noisepage             :   The main DBMS binary.
#######################################################################################################################

add_executable(noisepage src/main/terrier.cpp)
set_target_properties(noisepage PROPERTIES
        ENABLE_EXPORTS ON
        CXX_EXTENSIONS OFF)
target_link_libraries(noisepage noisepage_static)

#######################################################################################################################
# util_static           :   table_generator and table_reader for tests and benchmarks.
# TODO(WAN)             :   table_generator and table_reader are hacks, and the remaining cpp files are all TPL targets
#                           handled elsewhere. Kill this target once we refactor those hacks out of our system.
#######################################################################################################################
file(GLOB_RECURSE UTIL_SRCS CONFIGURE_DEPENDS
        ${PROJECT_SOURCE_DIR}/util/*.cpp
        ${PROJECT_SOURCE_DIR}/util/include/*.h)
list(REMOVE_ITEM UTIL_SRCS
        ${PROJECT_SOURCE_DIR}/util/execution/tpl.cpp
        ${PROJECT_SOURCE_DIR}/util/execution/gen_opt_bc.cpp
        ${PROJECT_SOURCE_DIR}/util/execution/bytecode_handlers_ir.cpp)
add_library(util_static STATIC ${UTIL_SRCS})
target_include_directories(util_static PUBLIC ${PROJECT_SOURCE_DIR}/util/include/)
target_link_libraries(util_static PUBLIC noisepage_static)

#######################################################################################################################
# gen_opt_bc            :   Generates optimized bitcode from the LLVM IR file generated as a tpl post-build target.
#######################################################################################################################

add_executable(gen_opt_bc util/execution/gen_opt_bc.cpp)
target_include_directories(gen_opt_bc PRIVATE ${LLVM_INCLUDE_DIRS})
target_link_libraries(gen_opt_bc PRIVATE ${LLVM_LIBRARIES})

#######################################################################################################################
# tpl                   :   Standalone binary for testing .tpl files and commands.
#                           Also generates bytecode_handlers_ir.bc.
#######################################################################################################################

# TODO(WAN): I don't see the point of doing this at CMake time. Another folder for what exactly?
set(TPL_VERSION_MAJOR 0)
set(TPL_VERSION_MINOR 1)
configure_file(util/execution/tpl.h.in "${PROJECT_BINARY_DIR}/execution/tpl.h") # Generate the header file.
add_executable(tpl util/execution/tpl.cpp)
set_target_properties(tpl PROPERTIES
        ENABLE_EXPORTS ON
        CXX_EXTENSIONS OFF
        )
# Build gen_opt_bc before building tpl.
add_dependencies(tpl gen_opt_bc)
target_include_directories(tpl PRIVATE ${PROJECT_BINARY_DIR} ${LLVM_INCLUDE_DIR})
target_link_libraries(tpl PUBLIC noisepage_static util_static ${LLVM_LIBRARIES})

# old flags for gen_opt_bc. TODO(WAN): make sure we match.
# -- /usr/bin/clang++-8 -std=c++17;;-mcx16;-march=native;-fvisibility=hidden;-ggdb;-O0;-fno-omit-frame-pointer;-fno-optimize-sibling-calls;-Wall;-Werror;-Wno-c++98-compat;-Wno-c++98-compat-pedantic;-fsanitize=address;-emit-llvm;-c -I/home/kapi/CLionProjects/terrier/cmake-build-debug/googletest_ep-prefix/src/googletest_ep/include;-I/home/kapi/CLionProjects/terrier/cmake-build-debug/gflags_ep-prefix/src/gflags_ep/include;-I/home/kapi/CLionProjects/terrier/cmake-build-debug/gbenchmark_ep/src/gbenchmark_ep-install/include;-I/usr/include;-I/usr/include/postgresql;-I/usr/lib/llvm-8/include;-I/home/kapi/CLionProjects/terrier/third_party/flatbuffers/include;-I/home/kapi/CLionProjects/terrier/src/include;-I/home/kapi/CLionProjects/terrier/third_party;-I/home/kapi/CLionProjects/terrier/test/include;-I/home/kapi/CLionProjects/terrier/util/include;-I/home/kapi/CLionProjects/terrier/benchmark/include;-I/home/kapi/CLionProjects/terrier/cmake-build-debug

message(STATUS "${CMAKE_BINARY_DIR}/CMakeFiles/bytecode_handlers_ir.dir/util/execution/bytecode_handlers_ir.cpp.o")

# This is a terrible hack because of CMake limitations: there is no way to use a different compiler for a
# single CMake target, and there is no easy way of grabbing the compiler flags that so-called "modern" CMake
# will apply. It is less headache overall to just specify all the flags that we want here...
list(APPEND HACK_CLANG_IR_FLAGS
        "-emit-llvm"                # Emit LLVM.
        "-c"                        # Only build an object .o file.
        "-std=c++17"
        "-mcx16"
        "-march=native"
        )

list(APPEND HACK_CLANG_IR_INCLUDES
        "-I${PROJECT_SOURCE_DIR}/include"
        "-I${LLVM_DIRS}"
        )

if (APPLE)
    # On macOS, the clang++ we want is not the clang++ in PATH. On Linux, it is.
    set(CLANG "${LLVM_TOOLS_BINARY_DIR}/clang++")
else ()
    find_program(CLANG NAMES "clang++-8" "clang++")
endif ()
message(STATUS "tpl ${CLANG} ${HACK_CLANG_IR_FLAGS} ${HACK_CLANG_IR_INCLUDES}")

add_custom_command(
        TARGET tpl
        POST_BUILD
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating Optimized Bitcode ... THIS DOESN'T WORK YET"
        COMMAND ${PROJECT_SOURCE_DIR}/build-support/gen_opt_bc_runner.sh ${CMAKE_BINARY_DIR}
        # ${CLANG} ${HACK_CLANG_IR_FLAGS} ${HACK_CLANG_IR_INCLUDES}
        # ${PROJECT_SOURCE_DIR}/util/execution/bytecode_handlers_ir.cpp -o bytecode_handlers_ir.bc
        COMMAND gen_opt_bc bytecode_handlers_ir.bc bytecode_handlers_opt.bc
        COMMAND mv bytecode_handlers_opt.bc bytecode_handlers_ir.bc
)

set_target_properties(
        tpl gen_opt_bc
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

add_custom_target(
        check-tpl
        DEPENDS tpl
        COMMAND ${BUILD_SUPPORT_DIR}/run_tpl_tests.py
        -b ${CMAKE_BINARY_DIR}/bin/tpl
        -f ${PROJECT_SOURCE_DIR}/sample_tpl/tpl_tests.txt
        -t ${PROJECT_SOURCE_DIR}/sample_tpl
        -d ${CMAKE_BINARY_DIR}/${BUILD_SUBDIR_NAME}
)

# end tpl

file(
        GLOB_RECURSE
        NOISEPAGE_TEST_UTIL_SRCS        # Store the list of test files into the variable ${NOISEPAGE_TEST_SRCS}.
        CONFIGURE_DEPENDS               # See above. Ask CMake to regenerate the build system (assuming support exists).
        ${PROJECT_SOURCE_DIR}/test/include/test_util/*.h
        ${PROJECT_SOURCE_DIR}/test/test_util/*.cpp
)

add_library(noisepage_test_util STATIC ${NOISEPAGE_TEST_UTIL_SRCS})
target_link_libraries(noisepage_test_util noisepage_static)
#
#
#add_library(test_util STATIC ${TEST_UTIL_SRCS})
#target_link_libraries(test_util terrier_static util_static ${TERRIER_LINK_LIBS})
#add_dependencies(test_util terrier_static util_static gtest gmock_main)


#MESSAGE(FATAL_ERROR "boo")
#
#
#
#add_executable(noisepage src/main/terrier.cpp)
#target_link_libraries(noisepage PRIVATE noisepage_lib)
#
#
## NoisePage uses C++17.
## NoisePage does not use special CXX extensions.
##set_target_properties(noisepage PROPERTIES CXX_EXTENSIONS OFF)
## Enable position independent code (-fPIC).
##set_target_properties(lib1 PROPERTIES POSITION_INDEPENDENT_CODE ON)
## Enable link-time optimization (LTO) which CMake calls inter-procedural optimization (IPO).
##include(CheckIPOSupported)
##check_ipo_supported(RESULT result)
##if(result)
##    set_target_properties(foo PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
##endif()
#
## Improve IDE support by allowing CMake to organize files by folders.
#set_property(GLOBAL PROPERTY USE_FOLDERS ON)
#
#MESSAGE(FATAL_ERROR "boo")
#
#
#include(CMakeParseArguments)
#include(ExternalProject)
#
#set(CMAKE_CXX_STANDARD 17)
#set(CMAKE_CXX_STANDARD_REQUIRED ON)
#
#include(GNUInstallDirs)
#
## Compatibility with CMake 3.1
#if (POLICY CMP0054)
#    # http://www.cmake.org/cmake/help/v3.1/policy/CMP0054.html
#    cmake_policy(SET CMP0054 NEW)
#endif ()
#
#if (APPLE)
#    cmake_policy(SET CMP0068 NEW)
#endif ()
#
#set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")
#set(BUILD_SUPPORT_DATA_DIR "${CMAKE_SOURCE_DIR}/build-support/data")
#
####################################################
## CMAKE OPTIONS
####################################################
#
#set(TERRIER_CXXFLAGS "" CACHE STRING
#        "Compiler flags to append when compiling Terrier")
#
#option(TERRIER_BUILD_STATIC
#        "Build the libterrier static libraries"
#        ON)
#
#option(TERRIER_BUILD_SHARED
#        "Build the libterrier shared libraries"
#        ON)
#
#option(TERRIER_TEST_MEMCHECK
#        "Run the test suite using valgrind --tool=memcheck"
#        OFF)
#
#option(TERRIER_BUILD_TESTS
#        "Build the Terrier googletest unit tests"
#        ON)
#
#option(TERRIER_BUILD_BENCHMARKS
#        "Build the Terrier micro benchmarks"
#        ON)
#
#option(TERRIER_RPATH_ORIGIN
#        "Build Terrier libraries with RATH set to \$ORIGIN"
#        OFF)
#
#option(TERRIER_INSTALL_NAME_RPATH
#        "Build Terrier libraries with install_name set to @rpath"
#        ON)
#
#option(TERRIER_GENERATE_COVERAGE
#        "Build with C++ code coverage enabled"
#        OFF)
#
#option(TERRIER_USE_JEMALLOC
#        "Dynamically link jemalloc as the memory allocator."
#        OFF)
#
#option(TERRIER_VERBOSE_THIRDPARTY_BUILD
#        "If off, output from ExternalProjects will be logged to files rather than shown"
#        OFF)
#
#option(TERRIER_VERBOSE_LINT
#        "If off, 'quiet' flags will be passed to linting tools"
#        OFF)
#
#if (NOT TERRIER_BUILD_TESTS)
#    set(NO_TESTS 1)
#endif ()
#
#if (NOT TERRIER_BUILD_BENCHMARKS)
#    set(NO_BENCHMARKS 1)
#endif ()
#
####################################################
## COMPILER TOOLS
####################################################
#
#set(CLANG_TOOLS_VERSION "8")
#find_package(ClangTools)
#if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
#    # Generate a Clang compile_commands.json "compilation database" file for use
#    # with various development tools, such as Vim's YouCompleteMe plugin.
#    # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
#    set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
#endif ()
#
#############################################################
## Compiler flags
#############################################################
#
## Determine compiler version
#include(CompilerInfo)
#include(SetupCxxFlags)
#
## ASAN / TSAN / UBSAN
#include(san-config)
#
############################################################
## code coverage
############################################################
#
#if (TERRIER_GENERATE_COVERAGE)
#
#    if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
#        message(FATAL_ERROR "Coverage can only be generated with a debug build type!")
#    endif ()
#
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage")
#endif ()
#
## Add common flags
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_COMMON_FLAGS}")
#
## For any C code, use the same flags.
#set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")
#
## Remove --std=c++17 to avoid errors from C compilers
#string(REPLACE "-std=c++17" "" CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
#
## Add C++-only flags, like -std=c++17
#set(CMAKE_CXX_FLAGS "${CXX_ONLY_FLAGS} ${CMAKE_CXX_FLAGS}")
#
## CMAKE_CXX_FLAGS now fully assembled
#message(STATUS "CXX_ONLY_FLAGS: ${CXX_ONLY_FLAGS}")
#message(STATUS "CXX_COMMON_FLAGS: ${CXX_COMMON_FLAGS}")
#message(STATUS "CXX_OPTIMIZATION_FLAGS: ${CXX_OPTIMIZATION_FLAGS}")
#message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
#message(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
#
#############################################################
## Dependencies
#############################################################
#
#include(BuildUtils)
#enable_testing()
#
#include(ThirdpartyToolchain)
#
############################################################
## "make check-lint" and "make check-censored" targets
############################################################
#if (NOT TERRIER_VERBOSE_LINT)
#    set(TERRIER_LINT_QUIET "--quiet")
#endif ()
#
#if (UNIX)
#
#    file(GLOB_RECURSE LINT_FILES
#            "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
#            "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
#            "${CMAKE_CURRENT_SOURCE_DIR}/test/*.h"
#            "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cpp"
#            "${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.h"
#            "${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.cpp"
#            "${CMAKE_CURRENT_SOURCE_DIR}/util/*.h"
#            "${CMAKE_CURRENT_SOURCE_DIR}/util/*.cpp"
#            )
#
#    find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
#    message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")
#
#    # Full lint
#    # Balancing act: cpplint.py takes a non-trivial time to launch,
#    # so process 12 files per invocation, while still ensuring parallelism
#    add_custom_target(check-lint echo '${LINT_FILES}' | xargs -n12 -P8
#            ${CPPLINT_BIN}
#            --verbose=2 ${TERRIER_LINT_QUIET}
#            --linelength=120
#            --filter=-legal/copyright,-build/header_guard
#            )
#
#    set(CENSOR_FILES ${LINT_FILES})
#
#    # If whitelisting ALL words becomes necessary, do it here. Make it harder so that people try not to do it.
#    # file(GLOB_RECURSE UNCENSORED_FILES
#    #        "${CMAKE_CURRENT_SOURCE_DIR}/src/..."
#    #        )
#    # list(REMOVE_ITEM CENSOR_FILES ${UNCENSORED_FILES})
#
#    add_custom_target(check-censored
#            grep --invert-match -n -e '^ *//' -e '^ *[*]' ${CENSOR_FILES} # check all uncommented lines w/ line num
#            | grep -i -f ${BUILD_SUPPORT_DATA_DIR}/bad_words.txt          # for bad words, not case sensitive
#            | grep --invert-match -e 'NOLINT'                             # the count of them that aren't NOLINT
#            # whitelist-start
#
#            # Description: allow usage of 'inline' in execution engine
#            # Reason: pmenon 2019/08/13
#            # All the bytecode handler functions have to be inline to ensure one definition.
#            # gen_opt_bc also cleans up the modules to add the odr to all functions.
#            # On top of that, many are marked ALWAYS_INLINE to force physical inlining into the VM.
#            # These are hand-selected by me based on profiling.
#            | grep --invert-match -e 'src/include/execution/.*:.*inline\\b.*'
#            | grep --invert-match -e 'src/execution/ast/context.cpp.*:.*inline\\b.*'
#            | grep --invert-match -e 'src/execution/vm/vm.cpp.*:.*inline\\b.*'
#
#            # Description: Regions, memory pool chunks, and vm stacks are managed using malloc and free
#            | grep --invert-match -e 'src/execution/sql/memory_pool.cpp:.*malloc\(.*'
#            | grep --invert-match -e 'src/execution/sql/memory_pool.cpp:.*calloc\(.*'
#            | grep --invert-match -e 'src/execution/sql/memory_pool.cpp:.*free\(.*'
#            | grep --invert-match -e 'src/execution/util/region.cpp:.*malloc\(.*'
#            | grep --invert-match -e 'src/execution/util/region.cpp:.*free\(.*'
#            | grep --invert-match -e 'src/execution/vm/vm.cpp:.*free\(.*'
#
#            # Description: This one is a false positive on the macro Op##AGG_TYPE##Free
#            | grep --invert-match -e 'src/execution/vm/vm.cpp:.*Free\(.*'
#
#            # Description: We're allowing shared_ptr and make_shared in testing folders.
#            # I don't actually remember the justification.
#            | grep --invert-match -e '${CMAKE_SOURCE_DIR}/test/.*shared_ptr'
#            | grep --invert-match -e '${CMAKE_SOURCE_DIR}/test/.*make_shared'
#
#            # whitelist-end
#            || exit 0                                                     # if nothing found, return 0
#            && exit 1                                                     # else return 1, note || && left-associative
#            )
#endif (UNIX)
#
############################################################
## "make format" and "make check-format" targets
############################################################
#
## we modified the format script to take multiple args
#
#string(CONCAT FORMAT_DIRS
#        "${CMAKE_CURRENT_SOURCE_DIR}/benchmark,"
#        "${CMAKE_CURRENT_SOURCE_DIR}/src,"
#        "${CMAKE_CURRENT_SOURCE_DIR}/test,"
#        "${CMAKE_CURRENT_SOURCE_DIR}/util"
#        )
#
## runs clang format and updates files in place.
#add_custom_target(format ${BUILD_SUPPORT_DIR}/run_clang_format.py
#        ${CLANG_FORMAT_BIN}
#        ${BUILD_SUPPORT_DATA_DIR}/clangformat_suppressions.txt
#        --source_dirs
#        ${FORMAT_DIRS}
#        --fix
#        ${TERRIER_LINT_QUIET})
#
## runs clang format and exits with a non-zero exit code if any files need to be reformatted
#add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run_clang_format.py
#        ${CLANG_FORMAT_BIN}
#        ${BUILD_SUPPORT_DATA_DIR}/clangformat_suppressions.txt
#        --source_dirs
#        ${FORMAT_DIRS}
#        ${TERRIER_LINT_QUIET})
#
############################################################
## "make check-clang-tidy" target
############################################################
#if (${CLANG_TIDY_FOUND})
#    # runs clang-tidy and exits with a non-zero exit code if any errors are found.
#    # note that clang-tidy automatically looks for a .clang-tidy file in parent directories
#    add_custom_target(check-clang-tidy
#            ${BUILD_SUPPORT_DIR}/run-clang-tidy.py                            # run LLVM's clang-tidy script
#            -clang-tidy-binary ${CLANG_TIDY_BIN}                              # using our clang-tidy binary
#            -p ${CMAKE_BINARY_DIR}                                            # using cmake's generated compile commands
#            )
#    if (TERRIER_BUILD_BENCHMARKS)
#        add_dependencies(check-clang-tidy benchmark gtest gflags)               # clang-tidy needs their headers to exist
#    elseif (TERRIER_BUILD_TESTS)
#        add_dependencies(check-clang-tidy gtest gflags)                         # clang-tidy needs their headers to exist
#    endif ()
#
#endif ()
#
############################################################
## Build properties
############################################################
#
## set compile output directory
#string(TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)
#
## If build in-source, create the latest symlink. If build out-of-source, which is
## preferred, simply output the binaries in the build folder
#if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
#    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}/")
#    # Link build/latest to the current build directory, to avoid developers
#    # accidentally running the latest debug build when in fact they're building
#    # release builds.
#    FILE(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
#    if (NOT APPLE)
#        set(MORE_ARGS "-T")
#    endif ()
#    EXECUTE_PROCESS(COMMAND ln ${MORE_ARGS} -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
#            ${CMAKE_CURRENT_BINARY_DIR}/build/latest)
#else ()
#    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}/")
#endif ()
#
## where to put generated archives (.a files)
#set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
#set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
#
## where to put generated libraries (.so files)
#set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
#set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
#
## where to put generated binaries
#set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")
#
#include_directories(${PROJECT_SOURCE_DIR}/src/include)
#if (${COMPILER_FAMILY} STREQUAL "gcc")
#    include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third_party)
#else ()
#    include_directories(${PROJECT_SOURCE_DIR}/third_party)
#endif ()
#include_directories(${PROJECT_SOURCE_DIR}/test/include)
#include_directories(${PROJECT_SOURCE_DIR}/util/include)
#include_directories(${PROJECT_SOURCE_DIR}/benchmark/include)
#include_directories(${PROJECT_BINARY_DIR})
#
## needed for BwTree
#if (CMAKE_COMPILER_IS_GNUCXX)
#    set(TERRIER_LINK_LIBS ${TERRIER_LINK_LIBS} atomic)
#endif ()
#
#set(TERRIER_LINK_LIBS
#        gflags
#        ${TERRIER_LINK_LIBS}
#        pg_query)
#
#set(TERRIER_DEPENDENCIES
#        gflags)
#
#set(TERRIER_TEST_LINK_LIBS
#        test_util
#        terrier_static
#        gtest
#        gmock_main
#        gflags
#        ${CMAKE_DL_LIBS})
#
#set(TERRIER_BENCHMARK_LINK_LIBS
#        benchmark_util
#        test_util
#        terrier_static
#        benchmark
#        gtest
#        gmock_main
#        gflags
#        ${CMAKE_DL_LIBS})
#
## configure settings for ctest
#configure_file(
#        "test/CTestCustom.cmake"
#        "${PROJECT_BINARY_DIR}/CTestCustom.cmake"
#        COPYONLY
#)
#
## ---[ Subdirectories
#add_subdirectory(src)
#add_subdirectory(test)
#add_subdirectory(benchmark)
#add_subdirectory(util)
