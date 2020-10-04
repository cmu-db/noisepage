# Resources for learning about _modern_ CMake:
# - https://llvm.org/docs/CMakePrimer.html LLVM's CMake guide. Start here.
# - https://gist.github.com/mbinna/c61dbb39bca0e4fb7d1f73b0d66a4fd1 Gist: This is essentially Pfeifer's talk.
# - https://www.youtube.com/watch?v=bsXLMQ6WgIk Talk: C++Now 2017 Daniel Pfeifer "Effective CMake".
# - https://www.youtube.com/watch?v=eC9-iRN2b04 Talk: CppCon2017 Mathieu Ropert "Using Modern CMake Patterns ...".
# - https://cliutils.gitlab.io/modern-cmake/    Book: Modern CMake. Some examples are no longer modern.
# - The official cmake.org site is full of outdated anti-patterns. Use for documentation, not for inspiration.

# Also, if you use CMake syntax for multiline comments, a kitten dies.
# If you didn't know that CMake has multiline comments, good.

# Explanations of various CMake quirks here.

#   1. A crash course on CMake's GLOB and GLOB_RECURSE.
#       CMake is not a build system -- CMake will not build your files.
#       CMake is a build system generator -- CMake will generate something (make, ninja, etc.) that builds your files.
#
#       How do you specify what files should get built?
#       - Well, you can either specify every single file manually, which is as horrible as it sounds. "Best practice".
#       - The old and not-recommended way is to use a GLOB, which creates a list of files.
#             Tangent: In CMake, a list is merely a ;-separated string. State of the art 2020 technology right there.
#
#       The problem with GLOB and GLOB_RECURSE is that the files are known to the build system _generator_, rather than
#       the build system. So you run GLOB and give a list of files to CMake. CMake hardcodes all these files when CMake
#       generates your build system. Later, you add a new .h or .cpp file, and your build system doesn't pick it up,
#       because the build system needs to be regenerated! Incidentally, CLion has a File > Reload CMake Project button
#       handy for this reason. To summarize the problem: "if you're hardcoding a list of files into your build system,
#       you're not going to pick up new files".
#
#       Well, specifying every single source file in CMake still sounds like a pain. Instead, the band-aid hack "modern"
#       solution is to specify CONFIGURE_DEPENDS. This means "if the build system supports this feature, if anything in
#       the GLOB changes, rerun CMake". In practice, our build system is Ninja or Make, and both support this.
#
#   2. Footgun warning: target_include_directories.
#       You must specify SYSTEM includes (typically disables warnings and errors from those includes) separately from
#       non-SYSTEM. In other words, always do
#           target_include_directories(target_name PUBLIC foo INTERFACE bar PRIVATE baz)
#           target_include_directories(target_name SYSTEM PUBLIC foo INTERFACE bar PRIVATE baz)
#       and never do
#           target_include_directories(target_name PUBLIC foo SYSTEM PRIVATE blah)
#       because the latter does not do what you think it does.
#
#   3. Footgun warning: FetchProject_ and friends automatically lowercase the project name.
#       If you're looking for ${someProject_SOURCE_DIR}, that capital letter will ruin your day.
#       Try doing string(TOLOWER ${someProject} someProject_LOWER) and using ${${someProject_LOWER}_SOURCE_DIR} instead.


# Ubuntu 20.04 ships with CMake version 3.16.3.
cmake_minimum_required(VERSION 3.16)

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

# Set the name of the CMake project to be NoisePage. This also automatically defines various magic variables,
# such as ${PROJECT_SOURCE_DIR} and ${PROJECT_BINARY_DIR}.
project(
        NoisePage
        # The version number of the NoisePage project.
        # The individual components can be extracted with ${PROJECT_VERSION_MAJOR}, ${PROJECT_VERSION_MINOR},
        # ${PROJECT_VERSION_PATCH}, and ${PROJECT_VERSION_TWEAK} respectively.
        VERSION 1.0.0.0
        DESCRIPTION "${NOISEPAGE_DESCRIPTION}"
        HOMEPAGE_URL "https://noise.page/"
        # NoisePage is a C++ project.
        LANGUAGES CXX
)

# Create a compile_commands.json file that can be easily parsed by build tools, clang-tidy, etc.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

#######################################################################################################################
# Safety checks.
#######################################################################################################################

# People keep running CMake in the wrong folder, completely nuking their project or creating weird bugs.
# This checks if you're running CMake from a folder that already has CMakeLists.txt.
# Importantly, this catches the common case of running it from the root directory.
file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" PATH_TO_CMAKELISTS_TXT)
if (EXISTS "${PATH_TO_CMAKELISTS_TXT}")
    message(FATAL_ERROR "Run CMake from a build subdirectory! \"mkdir build ; cd build ; cmake ..\" \
    Some junk files were created in this folder (CMakeCache.txt, CMakeFiles); you should delete those.")
endif ()

#######################################################################################################################
# Project definition.
#######################################################################################################################

# Print a welcome message with the project's version number.
message(STATUS
        "Welcome to NoisePage!\n\
        Home Page: ${PROJECT_HOMEPAGE_URL}\n\
        Version: ${PROJECT_VERSION}\n")

# CMAKE_MODULE_PATH is the search path for the include() and find_package() CMake commands.
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

message(STATUS "Compiler: ${CMAKE_CXX_COMPILER} ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

#######################################################################################################################
# CMake build types, specify with -DCMAKE_BUILD_TYPE={option}.
#   Debug (default), Release, RelWithDebInfo, FastDebug.
#   In practice people only use Debug or Release.
#
# CMake options, specify with -DNOISEPAGE_{option}=On.
#   NOISEPAGE_GENERATE_COVERAGE   : Enable C++ code coverage. Default OFF.
#   NOISEPAGE_USE_ASAN            : Enable ASAN, a fast memory error detector. Default OFF.
#   NOISEPAGE_USE_JEMALLOC        : Link with jemalloc instead of system malloc. Default OFF.
#
# CMake global variables.
#   BUILD_SUPPORT_DIR             : helper scripts for building belongs here.
#   BUILD_SUPPORT_DATA_DIR        : helper data for building belongs here.
#   NOISEPAGE_COMPILE_OPTIONS     : compile options to be added to NoisePage.
#   NOISEPAGE_LINK_OPTIONS        : link options to be added to NoisePage.
#   NOISEPAGE_INCLUDE_DIRECTORIES : include directories to be used for NoisePage.
#######################################################################################################################

# Default to DEBUG builds if -DCMAKE_BUILD_TYPE was not specified.
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif (NOT CMAKE_BUILD_TYPE)

option(NOISEPAGE_GENERATE_COVERAGE
        "Enable C++ code coverage."
        OFF)

option(NOISEPAGE_USE_ASAN
        "Enable ASAN, a fast memory error detector. https://clang.llvm.org/docs/AddressSanitizer.html"
        OFF)

option(NOISEPAGE_USE_JEMALLOC
        "Link jemalloc instead of system malloc. https://github.com/jemalloc/jemalloc"
        OFF)

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")
set(BUILD_SUPPORT_DATA_DIR "${CMAKE_SOURCE_DIR}/build-support/data")

# Everything else in this section will populate the following global variables.
set(NOISEPAGE_COMPILE_OPTIONS "")
set(NOISEPAGE_LINK_OPTIONS "")
set(NOISEPAGE_INCLUDE_DIRECTORIES "")

# Add compilation flags to NOISEPAGE_COMPILE_OPTIONS based on the current CMAKE_BUILD_TYPE.
string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-ggdb" "-O0" "-fno-omit-frame-pointer" "-fno-optimize-sibling-calls")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-ggdb" "-O1" "-fno-omit-frame-pointer" "-fno-optimize-sibling-calls")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-DNDEBUG" "-O3")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELWITHDEBINFO")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-ggdb" "-DNDEBUG" "-O2")
else ()
    message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

# Coverage.
if (${NOISEPAGE_GENERATE_COVERAGE})
    if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")                     # If coverage is required on non-DEBUG builds,
        message(FATAL_ERROR "Coverage requires a debug build type!")    # Then error out.
    endif ()
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "--coverage")     # Compile with coverage (compilers alias this).
    list(APPEND NOISEPAGE_LINK_OPTIONS "--coverage")        # Link coverage libraries (compilers alias this).
endif ()
message(STATUS "Coverage: ${NOISEPAGE_GENERATE_COVERAGE}")

# ASAN, which includes LSAN.
set(NOISEPAGE_ASAN_MSG "${NOISEPAGE_USE_ASAN}")
if (${NOISEPAGE_USE_ASAN})
    set(NOISEPAGE_ASAN_FLAGS
            "-fsanitize=address"                # Enable ASAN.
            "-fno-omit-frame-pointer"           # Nicer stack traces in error messages.
            "-fno-optimize-sibling-calls"       # Disable tail call elimination (perfect stack traces if inlining off).
            )
    list(APPEND NOISEPAGE_COMPILE_OPTIONS ${NOISEPAGE_ASAN_FLAGS})
    list(APPEND NOISEPAGE_LINK_OPTIONS ${NOISEPAGE_ASAN_FLAGS})
    unset(NOISEPAGE_ASAN_FLAGS)
    # Unfortunately, gcc does not support -fsanitize-blacklist. We ask users to set it manually as an env var.
    set(NOISEPAGE_ASAN_MSG "On (remember to set env ASAN_OPTIONS=${BUILD_SUPPORT_DATA_DIR}/sanitize_suppressions.txt)")
endif ()
message(STATUS "ASAN: ${NOISEPAGE_ASAN_MSG}")
unset(NOISEPAGE_ASAN_MSG)

# jemalloc.
set(NOISEPAGE_JEMALLOC_MSG "${NOISEPAGE_USE_JEMALLOC}")
if (${NOISEPAGE_USE_JEMALLOC})
    # We find jemalloc from the system to avoid building jemalloc from scratch.
    find_path(JEMALLOC_INCLUDE_DIR NAMES jemalloc/jemalloc.h REQUIRED)
    find_library(JEMALLOC_LIBRARIES NAMES jemalloc libjemalloc.so.1 libjemalloc.so.2 libjemalloc.dylib REQUIRED)
    list(APPEND NOISEPAGE_LINK_OPTIONS ${JEMALLOC_LIBRARIES})           # Add to NoisePage link libs.
    list(APPEND NOISEPAGE_INCLUDE_DIRECTORIES ${JEMALLOC_INCLUDE_DIR})  # Add to NoisePage includes.
    unset(JEMALLOC_INCLUDE_DIR)                                         # Variable hygiene.
    unset(JEMALLOC_LIBRARIES)                                           # Variable hygiene.
    set(NOISEPAGE_JEMALLOC_MSG "On (dir:${JEMALLOC_INCLUDE_DIR} lib:${JEMALLOC_LIBRARIES})")
endif ()
message(STATUS "jemalloc: ${NOISEPAGE_JEMALLOC_MSG}")
unset(NOISEPAGE_JEMALLOC_MSG)

list(APPEND NOISEPAGE_LINK_OPTIONS "-fuse-ld=lld")
message(STATUS "Linker: Will pass \"-fuse-ld=lld\" to linker.")

#######################################################################################################################
# Dependencies for finding dependencies. :)
#   VARS_BEFORE() / VARS_AFTER()    : Useful during CMake development to see what new variables got added.
#   message()                       : Redefined to suppress informational messages when MESSAGE_QUIET is on.
#   MESSAGE_QUIET                   : Flag controlling message() behavior. ON to suppress informational messages.
#######################################################################################################################

include(FetchContent)                   # FetchContent_ functions.
find_package(PkgConfig REQUIRED)        # pkg_search_module function.

# This pair of VARS_BEFORE() and VARS_AFTER() macros helps you to figure out what variables external code has defined.
# For example, suppose you want to include the nlohmann_json library with
#       find_package(nlohmann_json REQUIRED).
# What variables did it define? Is there a version number in there? Who knows?
#       VARS_BEFORE()
#       find_package(nlohmann_json REQUIRED).
#       VARS_AFTER()
# And now you know! This is useful if you're adding new libraries and want to print useful messages.

macro(VARS_BEFORE)
    get_directory_property(_vars_before VARIABLES)              # Save the current variables to _vars_before.
endmacro()

macro(VARS_AFTER)
    get_directory_property(_vars_after VARIABLES)               # Save the current variables to _vars_after.
    list(REMOVE_ITEM _vars_after _vars_before ${_vars_before})  # Diff _vars_after and _vars_before.
    foreach (_var IN LISTS _vars_after)                         # Print out all the variables that remain.
        message(STATUS "${_var} = ${${_var}}")
    endforeach ()
endmacro()

# Some libraries (cough Google Benchmark cough) just won't shut up, so this redefines the message function.
set(MESSAGE_QUIET OFF)
function(message)
    list(GET ARGV 0 MessageType)
    if (NOT MESSAGE_QUIET OR
            MessageType STREQUAL FATAL_ERROR OR
            MessageType STREQUAL SEND_ERROR OR
            MessageType STREQUAL WARNING OR
            MessageType STREQUAL AUTHOR_WARNING)
        list(REMOVE_AT ARGV 0)
        _message(${MessageType} "${ARGV}")
    endif ()
endfunction()

function(add_noisepage_dep NAME GIT_URL GIT_TAG)
    string(TOLOWER "${NAME}" NAME_LOWER)    # The automatically created variables are lowercase. Footgun!
    FetchContent_Declare(${NAME})           # Declare the resource to be fetched.
    if (NOT ${NAME}_POPULATED)
        # The long form of FetchContent_Populate is used since NoisePage requires control of the source folder name.
        # This is to enable includes like #include "NAME/their_files.h".
        FetchContent_Populate(${NAME}
                QUIET                                                   # Don't print verbose output while populating.
                SOURCE_DIR ${CMAKE_BINARY_DIR}/_deps/src/${NAME}        # Source files go here.
                BINARY_DIR ${CMAKE_BINARY_DIR}/_deps/build/${NAME}      # Build files go here.
                SUBBUILD_DIR ${CMAKE_BINARY_DIR}/_deps/sub/${NAME}      # Sub-build files go here.
                GIT_REPOSITORY ${GIT_URL}                               # Download from GIT_URL.
                GIT_TAG ${GIT_TAG}                                      # Checkout the specific GIT_TAG version.
                )

        # Whenever custom behavior is required, override with NoisePage third_party/deps_override/NAME_CMakeLists.txt.
        set(DEP_CUSTOM_CMAKELISTS ${PROJECT_SOURCE_DIR}/third_party/deps_override/${NAME}_CMakeLists.txt)
        if (EXISTS ${DEP_CUSTOM_CMAKELISTS})
            file(COPY ${DEP_CUSTOM_CMAKELISTS} DESTINATION ${${NAME_LOWER}_SOURCE_DIR})
            file(RENAME ${${NAME_LOWER}_SOURCE_DIR}/${NAME}_CMakeLists.txt ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt)
            message(STATUS "Copied ${DEP_CUSTOM_CMAKELISTS} -> ${${NAME_LOWER}_SOURCE_DIR}/CMakeLists.txt.")
        endif ()
        unset(DEP_CUSTOM_CMAKELISTS)

        set(MESSAGE_QUIET ON)                                                           # Silence third party includes.
        add_subdirectory(${${NAME_LOWER}_SOURCE_DIR} ${${NAME_LOWER}_BINARY_DIR})       # Include the dependency.
        unset(MESSAGE_QUIET)                                                            # Undo silence.
    endif ()

    # Get the date of the last git commit for the GIT_TAG branch.
    execute_process(COMMAND git log -1 --format=%cd --date=short                        # Get the last commit's date.
            WORKING_DIRECTORY ${${NAME_LOWER}_SOURCE_DIR}                               # From the specified git dir.
            OUTPUT_VARIABLE GIT_LAST_COMMIT)                                            # Save to GIT_LAST_COMMIT.
    string(STRIP "${GIT_LAST_COMMIT}" GIT_LAST_COMMIT)                                  # Remove any trailing newlines.

    message(STATUS "[FOUND] ${NAME} (${GIT_URL} ${GIT_TAG} ${GIT_LAST_COMMIT})")        # Print a success message.
endfunction()

function(add_noisepage_dep_singlefile NAME FILE_URL)
    string(TOLOWER "${NAME}" NAME_LOWER)    # For consistency with add_noisepage_dep above, also use lowercase.
    get_filename_component(FILE_NAME ${FILE_URL} NAME)                                  # Get the filename.
    file(DOWNLOAD ${FILE_URL} ${CMAKE_BINARY_DIR}/_deps/src/${NAME}/${FILE_NAME})       # Download the file.
    set(${NAME_LOWER}_SOURCE_DIR ${CMAKE_BINARY_DIR}/_deps/src/${NAME} PARENT_SCOPE)    # Set the expected SOURCE_DIR.
    message(STATUS "[FOUND] ${NAME} (${FILE_URL})")                                     # Print a success message.
endfunction()

#######################################################################################################################
# Dependencies.
#######################################################################################################################

message(STATUS "\nDependencies: Locating...\n==========================================\n")

# DISGUSTING HACK: Annoyingly, -fPIC is required on pretty much every dependency.
set(OLD_CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")           # Save the current CMAKE_C_FLAGS.
set(OLD_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")       # Save the current CMAKE_CXX_FLAGS.
set(CMAKE_C_FLAGS "-fPIC")                          # Add -fPIC to dependencies.
set(CMAKE_CXX_FLAGS "-fPIC")                        # Add -fPIC to dependencies.

# Google Benchmark build settings.
set(BENCHMARK_ENABLE_TESTING OFF)                   # Disable testing of the benchmark library.
set(BENCHMARK_ENABLE_EXCEPTIONS OFF)                # Disable the use of exceptions in the benchmark library.
set(BENCHMARK_ENABLE_LTO OFF)                       # Disable link time optimisation of the benchmark library.
set(BENCHMARK_USE_LIBCXX OFF)                       # Disable building and testing using libc++ as the standard library.
set(BENCHMARK_BUILD_32_BITS OFF)                    # Disable building a 32 bit version of the library.
set(BENCHMARK_ENABLE_INSTALL OFF)                   # Disable installation of benchmark.
set(BENCHMARK_DOWNLOAD_DEPENDENCIES OFF)            # Disable downloading and building google benchmark dependencies.
set(BENCHMARK_ENABLE_GTEST_TESTS OFF)               # Disable building benchmark unit tests.

# gflags build settings.
set(GFLAGS_BUILD_SHARED_LIBS OFF)                   # Disable building gflags shared library.
set(GFLAGS_BUILD_STATIC_LIBS ON)                    # Enable building gflags static library.
set(GFLAGS_BUILD_gflags_LIB ON)                     # Enable building gflags as a library.
set(GFLAGS_BUILD_gflags_nothreads_LIB OFF)          # Disable building the nothreads version of gflags.
set(GFLAGS_BUILD_TESTING OFF)                       # Disable building gflags tests.
set(GFLAGS_BUILD_PACKAGING OFF)                     # Disable building gflags packaging.

# Include the source of the dependencies as sources that NoisePage can include.
list(APPEND NOISEPAGE_INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/_deps/src/)

# Fetch single-file dependencies.
add_noisepage_dep_singlefile(csv https://raw.githubusercontent.com/vincentlaucsb/csv-parser/92694230ba4863a436b6533788e590fce70b5f44/single_include/csv.hpp)
add_noisepage_dep_singlefile(portable_endian https://gist.githubusercontent.com/panzi/6856583/raw/1eca2ab34f2301b9641aa73d1016b951fff3fc39/portable_endian.h)

# Fetch project dependencies.
add_noisepage_dep(count https://github.com/pmenon/libcount.git 6eef9d048d4577f144506ffc076c1913f8faf3ef)
add_noisepage_dep(gflags https://github.com/gflags/gflags.git v2.2.2)
add_noisepage_dep(googlebenchmark https://github.com/google/benchmark.git v1.5.2)
add_noisepage_dep(googletest https://github.com/google/googletest.git release-1.10.0)
add_noisepage_dep(ips4o https://github.com/ips4o/ips4o.git 2fb65ca11ac1898faee2f146610e6409489d2105)
add_noisepage_dep(madoka https://github.com/s-yata/madoka.git 66783ee5b84a432f934517ad65452d54b19230bb)
add_noisepage_dep(nlohmann_json https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git v3.7.3)
add_noisepage_dep(spdlog https://github.com/gabime/spdlog.git v1.8.1)
add_noisepage_dep(xbyak https://github.com/herumi/xbyak.git v5.77)
add_noisepage_dep(xxHash https://github.com/Cyan4973/xxHash.git v0.8.0)

# Handle other dependencies.

# TODO(WAN): libpg_query is CURSED. Someone else is welcome to fix it. Or I may retry in the future.
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/libpg_query/ EXCLUDE_FROM_ALL)

# libpqxx.
find_library(PQXX_LIBRARIES NAMES pqxx libpqxx REQUIRED)
find_path(PQXX_INCLUDE_DIRECTORIES NAMES pqxx/pqxx REQUIRED)
list(APPEND NOISEPAGE_LINK_OPTIONS ${PQXX_LIBRARIES})
list(APPEND NOISEPAGE_INCLUDE_DIRECTORIES ${PQXX_INCLUDE_DIRECTORIES})
message(STATUS "[FOUND] pqxx (dir:${PQXX_INCLUDE_DIRECTORIES} lib:${PQXX_LIBRARIES})")

# libevent.
pkg_search_module(EVENT REQUIRED libevent)
pkg_search_module(EVENT_PTHREADS REQUIRED libevent_pthreads)

# LLVM 8.0.
find_package(LLVM 8.0 REQUIRED CONFIG)
message(STATUS "[FOUND] LLVM ${LLVM_PACKAGE_VERSION}")
# Explicitly request all of the LLVM components that we want.
llvm_map_components_to_libnames(LLVM_LIBRARIES core ipo mcjit nativecodegen native support)

# TBB.
find_package(TBB)
message(STATUS "[FOUND] TBB ${TBB_VERSION}")

# DISGUSTING HACK: Restore the old CMAKE_C_FLAGS, CMAKE_CXX_FLAGS, and CMAKE_BUILD_TYPE.
set(CMAKE_C_FLAGS "${OLD_CMAKE_C_FLAGS}")           # Restore the old CMAKE_C_FLAGS.
set(CMAKE_CXX_FLAGS "${OLD_CMAKE_CXX_FLAGS}")       # Restore the old CMAKE_CXX_FLAGS.
unset(OLD_CMAKE_C_FLAGS)                            # Variable hygiene.
unset(OLD_CMAKE_CXX_FLAGS)                          # Variable hygiene.

message(STATUS "\n==========================================\nDependencies: All found!\n")

#######################################################################################################################
# noisepage_objlib      :   NoisePage object library, built once and linked into both static and shared targets.
# noisepage_static      :   All of NoisePage functionality exposed as a static library.
# noisepage_shared      :   All of NoisePage functionality exposed as a shared library.
#######################################################################################################################

# Get the list of all NoisePage sources.
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

# Build NoisePage as an OBJECT library first, i.e., a .o file per corresponding .cpp file.
# The OBJECT library is built first so that the same .o files can be linked into static and shared libraries.
# This allows both noisepage_static and noisepage_shared to be built with a single compilation of translation units.
add_library(noisepage_objlib OBJECT ${NOISEPAGE_SRCS})

set_target_properties(noisepage_objlib PROPERTIES
        POSITION_INDEPENDENT_CODE ON                # Required for static linking into other shared libraries.
        CXX_EXTENSIONS OFF                          # Disable compiler extensions (e.g., use c++17 not gnu17).
        )
target_compile_options(noisepage_objlib PRIVATE     # PRIVATE: only noisepage_objlib uses the following.
        "-Werror"                                   # Treat warnings as errors.
        "-Wall"                                     # Enable "all" warnings. (Not actually all warnings.)
        )
target_compile_options(noisepage_objlib PUBLIC      # PUBLIC: all consumers of the library inherit the following.
        "-march=native"                             # Enable machine-specific instruction sets and optimizations.
        "-mcx16"                                    # Allow CMPXCHG16B (16-byte compare and exchange).
        ${NOISEPAGE_COMPILE_OPTIONS}
        )
target_link_options(noisepage_objlib PUBLIC         # PUBLIC: all consumers of the library inherit the following.
        "-march=native"                             # Enable machine-specific instruction sets and optimizations.
        ${NOISEPAGE_LINK_OPTIONS}
        )
target_compile_features(noisepage_objlib PUBLIC     # PUBLIC: all consumers of the library inherit the following.
        cxx_std_17                                  # Require support for C++17.
        )
target_include_directories(noisepage_objlib PUBLIC  # PUBLIC: all consumers of the library inherit the following.
        ${PROJECT_SOURCE_DIR}/src/include           # Include NoisePage src/include/ headers.
        ${NOISEPAGE_INCLUDE_DIRECTORIES}
        )
target_include_directories(noisepage_objlib SYSTEM PUBLIC   # SYSTEM PUBLIC: inherit without error checking.
        ${LLVM_INCLUDE_DIRS}                                # Include LLVM headers.
        )
target_link_libraries(noisepage_objlib PUBLIC       # PUBLIC: all consumers of the library inherit the following.
        count
        gflags
        ips4o::ips4o
        madoka::madoka
        nlohmann_json::nlohmann_json
        pg_query::pg_query
        spdlog::spdlog
        xbyak::xbyak
        xxHash::xxhash
        ${EVENT_LINK_LIBRARIES}
        ${EVENT_PTHREADS_LINK_LIBRARIES}
        ${LLVM_LIBRARIES}
        ${TBB_LIBRARIES_RELEASE}
        )

# Create the noisepage_static and noisepage_shared libraries using the objects from noisepage_objlib.
add_library(noisepage_static STATIC $<TARGET_OBJECTS:noisepage_objlib>)     # Bundle up these objects into static lib.
target_link_libraries(noisepage_static PUBLIC noisepage_objlib)             # Consumers will inherit this link.
add_library(noisepage_shared SHARED $<TARGET_OBJECTS:noisepage_objlib>)     # Bundle up these objects into shared lib.
target_link_libraries(noisepage_shared PUBLIC noisepage_objlib)             # Consumers will inherit this link.

#######################################################################################################################
# noisepage             :   The main DBMS binary.
#######################################################################################################################

add_executable(noisepage src/main/terrier.cpp)
target_link_libraries(noisepage noisepage_static)
set_target_properties(noisepage PROPERTIES CXX_EXTENSIONS OFF)

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
set_target_properties(util_static PROPERTIES CXX_EXTENSIONS OFF)

#######################################################################################################################
# gen_opt_bc            :   Generates optimized bitcode from the LLVM IR file generated as a tpl post-build target.
#######################################################################################################################

add_executable(gen_opt_bc util/execution/gen_opt_bc.cpp)
target_include_directories(gen_opt_bc PRIVATE ${LLVM_INCLUDE_DIRS})
target_link_libraries(gen_opt_bc PRIVATE ${LLVM_LIBRARIES})
set_target_properties(
        gen_opt_bc
        PROPERTIES
        CXX_EXTENSIONS OFF
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

#######################################################################################################################
# hack_bytecode_handlers_ir     :   This target is not actually used to build the bytecode handlers, because the
#                                   bytecode handlers have to be built with clang and it is possible that the current
#                                   compiler is gcc -- you simply cannot mix two compilers in one CMake project
#                                   without jumping through SuperProject hoops. The target is created to get the
#                                   relevant entry in compile_commands.json, which is then parsed and run manually
#                                   with clang... :(
#######################################################################################################################

add_library(hack_bytecode_handlers_ir util/execution/bytecode_handlers_ir.cpp)
target_link_libraries(hack_bytecode_handlers_ir PRIVATE noisepage_static)
set_target_properties(hack_bytecode_handlers_ir PROPERTIES CXX_EXTENSIONS OFF)

#######################################################################################################################
# tpl                   :   Standalone binary for testing .tpl files and commands.
#                           Also generates POST_BUILD bytecode_handlers_ir.bc.
#######################################################################################################################

add_executable(tpl util/execution/tpl.cpp)
add_dependencies(tpl gen_opt_bc hack_bytecode_handlers_ir)      # Build these before building tpl.
target_include_directories(tpl PRIVATE ${LLVM_INCLUDE_DIR})
target_link_libraries(tpl PUBLIC noisepage_static util_static ${LLVM_LIBRARIES})
set_target_properties(
        tpl
        PROPERTIES
        CXX_EXTENSIONS OFF
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# On MacOS, the clang++ we want is not the clang++ in PATH. On Linux, it is.
if (APPLE)
    set(GEN_OPT_BC_CLANG "${LLVM_TOOLS_BINARY_DIR}/clang++")
else ()
    find_program(GEN_OPT_BC_CLANG NAMES "clang++-8" "clang++" REQUIRED)
endif ()

add_custom_command(
        TARGET tpl
        DEPENDS "${PROJECT_SOURCE_DIR}/src/include/execution/vm/bytecodes.h" "${PROJECT_SOURCE_DIR}/src/include/execution/vm/bytecode_handlers.h"
        POST_BUILD
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMAND cmake -E echo "Generating optimized bitcode ..."
        COMMAND cmake -E echo "Running: ${PROJECT_SOURCE_DIR}/build-support/gen_opt_bc_runner.py ${GEN_OPT_BC_CLANG} ${CMAKE_BINARY_DIR} ${PROJECT_SOURCE_DIR}/util/execution/bytecode_handlers_ir.cpp ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_ir.bc"
        COMMAND ${PROJECT_SOURCE_DIR}/build-support/gen_opt_bc_runner.py ${GEN_OPT_BC_CLANG} ${CMAKE_BINARY_DIR} ${PROJECT_SOURCE_DIR}/util/execution/bytecode_handlers_ir.cpp ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_ir.bc
        COMMAND cmake -E echo "Running: ${CMAKE_BINARY_DIR}/bin/gen_opt_bc ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_ir.bc ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_opt.bc"
        COMMAND ${CMAKE_BINARY_DIR}/bin/gen_opt_bc ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_ir.bc ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_opt.bc
        COMMAND cmake -E echo "Running: mv ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_opt.bc ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_ir.bc"
        COMMAND mv ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_opt.bc ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_ir.bc
        COMMAND cmake -E echo "Generated optimized bitcode at ${CMAKE_BINARY_DIR}/bin/bytecode_handlers_ir.bc."
        USES_TERMINAL
)

#######################################################################################################################
# Tests.
#######################################################################################################################

enable_testing()        # CTest support is built into CMake.
include(GoogleTest)     # We rely on GoogleTest for all our tests.
configure_file(test/CTestCustom.cmake ${PROJECT_BINARY_DIR}/CTestCustom.cmake COPYONLY) # Configure CTest output.

file(GLOB_RECURSE
        NOISEPAGE_TEST_UTIL_SRCS
        CONFIGURE_DEPENDS
        ${PROJECT_SOURCE_DIR}/test/include/test_util/*.h
        ${PROJECT_SOURCE_DIR}/test/test_util/*.cpp
        )

add_library(noisepage_test_util STATIC ${NOISEPAGE_TEST_UTIL_SRCS})
target_link_libraries(noisepage_test_util PUBLIC noisepage_shared gtest gmock util_static pqxx)
target_include_directories(noisepage_test_util PUBLIC ${PROJECT_SOURCE_DIR}/test/include/)
set_target_properties(noisepage_test_util PROPERTIES CXX_EXTENSIONS OFF)

function(add_noisepage_test TEST_NAME TEST_CPP)
    set(TEST_OUTPUT_DIR "${CMAKE_BINARY_DIR}/test")             # Output directory for tests.

    add_executable(${TEST_NAME} ${TEST_CPP})
    target_link_libraries(${TEST_NAME} PRIVATE gtest gmock gmock_main noisepage_shared ${ARGN})
    set_target_properties(${TEST_NAME} PROPERTIES
            CXX_EXTENSIONS OFF                                  # Disable compiler-specific extensions.
            RUNTIME_OUTPUT_DIRECTORY "${TEST_OUTPUT_DIR}"       # Output the test binaries to this folder.
            )
    target_include_directories(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/test/include/)
    # TODO(WAN): The "modern" gtest_discover_test has a ton of files. Favoring legacy add_test for now...
    add_test(${TEST_NAME} ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} test ${TEST_OUTPUT_DIR}/${TEST_NAME})
    # Label each test with "unittest" so that ctest can run all the tests under the "unittest" label later.
    set_tests_properties(${TEST_NAME} PROPERTIES LABELS "unittest")
endfunction()

file(GLOB_RECURSE NOISEPAGE_TEST_SOURCES
        "test/binder/*.cpp"
        "test/catalog/*.cpp"
        "test/common/*.cpp"
        "test/execution/*.cpp"
        "test/integration/*.cpp"
        "test/metrics/*.cpp"
        "test/network/*.cpp"
        "test/optimizer/*.cpp"
        "test/parser/*.cpp"
        "test/planner/*.cpp"
        "test/settings/*.cpp"
        "test/storage/*.cpp"
        "test/traffic_cop/*.cpp"
        "test/transaction/*.cpp"
        "test/type/*.cpp"
        )

add_custom_target(unittest ctest -L unittest --no-compress-output -T Test)
foreach (NOISEPAGE_TEST_CPP ${NOISEPAGE_TEST_SOURCES})
    file(RELATIVE_PATH NOISEPAGE_TEST_CPP_REL "${PROJECT_SOURCE_DIR}/test" ${NOISEPAGE_TEST_CPP})
    get_filename_component(NOISEPAGE_TEST_DIR ${NOISEPAGE_TEST_CPP_REL} DIRECTORY)

    set(NOISEPAGE_TEST_LINKS "")

    if ("${NOISEPAGE_TEST_DIR}" MATCHES "^(network|traffic_cop)$")
        set(NOISEPAGE_TEST_LINKS "pqxx")
    endif ()

    if ("${NOISEPAGE_TEST_DIR}" MATCHES "^(execution|integration|optimizer|storage|transaction)$")
        set(NOISEPAGE_TEST_LINKS "noisepage_test_util")
    endif ()

    get_filename_component(NOISEPAGE_TEST ${NOISEPAGE_TEST_CPP} NAME_WE)
    add_noisepage_test(${NOISEPAGE_TEST} ${NOISEPAGE_TEST_CPP} ${NOISEPAGE_TEST_LINKS})
    add_dependencies(unittest ${NOISEPAGE_TEST})
endforeach ()

#######################################################################################################################
# Benchmarks.
#######################################################################################################################

file(GLOB_RECURSE
        NOISEPAGE_BENCHMARK_UTIL_SRCS
        CONFIGURE_DEPENDS
        ${PROJECT_SOURCE_DIR}/benchmark/include/benchmark_util/*.h
        ${PROJECT_SOURCE_DIR}/benchmark/benchmark_util/*.cpp
        )

add_library(noisepage_benchmark_util STATIC ${NOISEPAGE_BENCHMARK_UTIL_SRCS})
target_link_libraries(noisepage_benchmark_util PUBLIC noisepage_shared gtest gmock gmock_main util_static noisepage_test_util pqxx benchmark)
target_include_directories(noisepage_benchmark_util PUBLIC ${PROJECT_SOURCE_DIR}/benchmark/include)
set_target_properties(noisepage_benchmark_util PROPERTIES CXX_EXTENSIONS OFF)

set(NOISEPAGE_BENCHMARKS "")
macro(add_noisepage_benchmark BENCHMARK_NAME BENCHMARK_CPP)
    add_executable(${BENCHMARK_NAME} ${BENCHMARK_CPP})
    target_link_libraries(${BENCHMARK_NAME} PRIVATE noisepage_benchmark_util ${ARGN})
    target_link_directories(${BENCHMARK_NAME} PRIVATE ${CMAKE_BINARY_DIR})
    target_compile_options(${BENCHMARK_NAME} PRIVATE "-fPIC")
    set_target_properties(${BENCHMARK_NAME} PROPERTIES
            CXX_EXTENSIONS OFF
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark"
            )
    target_include_directories(${BENCHMARK_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/benchmark/include/)

    set(BENCHMARK_PATH "${CMAKE_BINARY_DIR}/benchmark/${BENCHMARK_NAME}")
    add_test(${BENCHMARK_NAME}
            ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} benchmark ${BENCHMARK_PATH} ${NO_COLOR})
    set_tests_properties(${BENCHMARK_NAME} PROPERTIES LABELS "benchmark")
    list(APPEND NOISEPAGE_BENCHMARK ${BENCHMARK_NAME})
endmacro()


file(GLOB_RECURSE NOISEPAGE_BENCHMARK_SOURCES
        "benchmark/catalog/*.cpp"
        "benchmark/common/*.cpp"
        "benchmark/integration/*.cpp"
        "benchmark/metrics/*.cpp"
        "benchmark/parser/*.cpp"
        "benchmark/storage/*.cpp"
        "benchmark/transaction/*.cpp"
        "benchmark/runner/*.cpp"
        )

add_custom_target(runbenchmark ctest -L benchmark --no-compress-output -T Test)
foreach (NOISEPAGE_BENCHMARK_CPP ${NOISEPAGE_BENCHMARK_SOURCES})
    file(RELATIVE_PATH NOISEPAGE_BENCHMARK_CPP_REL "${PROJECT_SOURCE_DIR}/benchmark" ${NOISEPAGE_BENCHMARK_CPP})
    get_filename_component(NOISEPAGE_BENCHMARK_DIR ${NOISEPAGE_BENCHMARK_CPP_REL} DIRECTORY)

    get_filename_component(NOISEPAGE_BENCHMARK ${NOISEPAGE_BENCHMARK_CPP} NAME_WE)
    add_noisepage_benchmark(${NOISEPAGE_BENCHMARK} ${NOISEPAGE_BENCHMARK_CPP} ${NOISEPAGE_BENCHMARK_LINKS})
    add_dependencies(runbenchmark ${NOISEPAGE_BENCHMARK})
endforeach ()

#######################################################################################################################
# Useful some day, maybe.
#######################################################################################################################

# Some day, CLion will probably support USE_FOLDERS in the IDE view of CMake targets.
# When that day comes, clump all the "you shouldn't need to build this manually" targets into a folder like so.
# set_property(GLOBAL PROPERTY USE_FOLDERS ON)
# set_target_properties(hack_bytecode_handlers_ir PROPERTIES FOLDER HiddenTargets)

#######################################################################################################################
# Global variables for helper scripts.
# LINT_FILES    :   All the files that are to be linted. Exposed since other tools may want to use them.
#######################################################################################################################

file(GLOB_RECURSE LINT_FILES
        "${PROJECT_SOURCE_DIR}/src/*.h"
        "${PROJECT_SOURCE_DIR}/src/*.cpp"
        "${PROJECT_SOURCE_DIR}/test/*.h"
        "${PROJECT_SOURCE_DIR}/test/*.cpp"
        "${PROJECT_SOURCE_DIR}/benchmark/*.h"
        "${PROJECT_SOURCE_DIR}/benchmark/*.cpp"
        "${PROJECT_SOURCE_DIR}/util/*.h"
        "${PROJECT_SOURCE_DIR}/util/*.cpp"
        )

#######################################################################################################################
# check-censored : Run a python script.
#######################################################################################################################

# If allowing entire files becomes necessary, do it here. Make it harder so that people try not to do it.
# file(GLOB_RECURSE UNCENSORED_FILES "${PROJECT_SOURCE_DIR}/src/...")
# list(REMOVE_ITEM CENSOR_FILES ${UNCENSORED_FILES})

set(BAD_WORDS_FILE ${BUILD_SUPPORT_DATA_DIR}/bad_words.txt)
if (NOT EXISTS ${BAD_WORDS_FILE})
    message(STATUS "[MISSING] bad words at ${BUILD_SUPPORT_DATA_DIR}/bad_words.txt, no check-censored.")
else ()
    add_custom_target(check-censored                                        # Make the "check-censored" target.
            grep --invert-match -n -e '^ *//' -e '^ *[*]' ${LINT_FILES}     # Check all uncommented lines with line num.
            | grep -f ${BAD_WORDS_FILE}                                     # For bad words, case sensitive.
            | grep --invert-match -e 'NOLINT'                               # And that are not marked as NOLINT.

            # Start of allowlist.

            # Description: allow usage of 'inline' in execution engine.
            # Reason: pmenon 2019/08/13
            # All the bytecode handler functions have to be inline to ensure one definition.
            # gen_opt_bc also cleans up the modules to add the odr to all functions.
            # On top of that, many are marked ALWAYS_INLINE to force physical inlining into the VM.
            # These are hand-selected by Prashanth based on profiling.
            | grep --invert-match -e 'src/include/execution/.*:.*inline\\b.*'
            | grep --invert-match -e 'src/execution/ast/context.cpp.*:.*inline\\b.*'
            | grep --invert-match -e 'src/execution/vm/vm.cpp.*:.*inline\\b.*'

            # Description: Regions, memory pool chunks, and vm stacks are managed using malloc and free.
            | grep --invert-match -e 'src/execution/sql/memory_pool.cpp:.*malloc\(.*'
            | grep --invert-match -e 'src/execution/sql/memory_pool.cpp:.*calloc\(.*'
            | grep --invert-match -e 'src/execution/sql/memory_pool.cpp:.*free\(.*'
            | grep --invert-match -e 'src/execution/util/region.cpp:.*malloc\(.*'
            | grep --invert-match -e 'src/execution/util/region.cpp:.*free\(.*'
            | grep --invert-match -e 'src/execution/vm/vm.cpp:.*free\(.*'

            # End of allowlist.

            || exit 0                                                     # If nothing found, return 0.
            && exit 1                                                     # Else return 1, note || && left-associative.
            USES_TERMINAL
            )
    message(STATUS "[ADDED] check-censored")
endif ()
unset(BAD_WORDS_FILE)

#######################################################################################################################
# check-lint    :   Run the cpplint python script.
#######################################################################################################################

add_noisepage_dep_singlefile(cpplint https://raw.githubusercontent.com/cpplint/cpplint/5b4259ef4c94d34e98192f53466c8af5e9d1c259/cpplint.py)
find_program(CPPLINT_BIN NAMES cpplint.py HINTS ${cpplint_SOURCE_DIR})

if ("${CPPLINT_BIN}" STREQUAL "CPPLINT_BIN-NOTFOUND")
    message(STATUS "[MISSING] cpplint at ${cpplint_SOURCE_DIR}/cpplint.py, no check-lint.")
else ()
    # Balancing act: cpplint.py takes a non-trivial time to launch, so process 12 files per invocation with parallelism.
    add_custom_target(check-lint
            COMMENT "Running: echo LINT_FILES | xargs -n12 -P8 python3 ${CPPLINT_BIN} --verbose=2 --linelength=120 --quiet --filter=legal/copyright,-build/header_guard"
            COMMAND echo '${LINT_FILES}' | xargs -n12 -P8
            python3 ${CPPLINT_BIN}
            --verbose=2 --linelength=120 --quiet
            --filter=-legal/copyright,-build/header_guard
            USES_TERMINAL
            )
    message(STATUS "[ADDED] check-lint (${CPPLINT_BIN})")
endif ()
unset(${CPPLINT_BIN})

#######################################################################################################################
# check-tpl     :   Python script to check core TPL functionality.
#######################################################################################################################

add_custom_target(
        check-tpl
        DEPENDS tpl
        COMMAND cmake -E echo "Running: ${BUILD_SUPPORT_DIR}/run_tpl_tests.py \
            -b ${CMAKE_BINARY_DIR}/bin/tpl \
            -f ${PROJECT_SOURCE_DIR}/sample_tpl/tpl_tests.txt \
            -t ${PROJECT_SOURCE_DIR}/sample_tpl \
            -d ${CMAKE_BINARY_DIR}/bin"
        COMMAND ${BUILD_SUPPORT_DIR}/run_tpl_tests.py
        -b ${CMAKE_BINARY_DIR}/bin/tpl
        -f ${PROJECT_SOURCE_DIR}/sample_tpl/tpl_tests.txt
        -t ${PROJECT_SOURCE_DIR}/sample_tpl
        -d ${CMAKE_BINARY_DIR}/bin
        USES_TERMINAL
)
message(STATUS "[ADDED] check-tpl")

#######################################################################################################################
# format            :   Reformat the codebase according to standards.
# check-format      :   Check if the codebase is formatted according to standards.
#######################################################################################################################

# we modified the format script to take multiple args

string(CONCAT FORMAT_DIRS
        "${CMAKE_CURRENT_SOURCE_DIR}/benchmark,"
        "${CMAKE_CURRENT_SOURCE_DIR}/src,"
        "${CMAKE_CURRENT_SOURCE_DIR}/test,"
        "${CMAKE_CURRENT_SOURCE_DIR}/util"
        )

# runs clang format and updates files in place.
add_custom_target(format
        ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${BUILD_SUPPORT_DATA_DIR}/clangformat_suppressions.txt
        --source_dirs
        ${FORMAT_DIRS}
        --fix
        --quiet
        USES_TERMINAL
        )

# runs clang format and exits with a non-zero exit code if any files need to be reformatted
add_custom_target(check-format
        ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        ${BUILD_SUPPORT_DATA_DIR}/clangformat_suppressions.txt
        --source_dirs
        ${FORMAT_DIRS}
        --quiet
        USES_TERMINAL
        )

unset(FORMAT_DIRS)

#######################################################################################################################
# check-clang-tidy     :   Run clang-tidy static analysis on the codebase.
#######################################################################################################################

find_program(CLANG_TIDY_BIN NAMES clang-tidy-8 clang-tidy)
if ("${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND")
    message(STATUS "[MISSING] clang-tidy not found, no check-clang-tidy.")
else ()
    # Run clang-tidy and exit with a non-zero exit code if any errors are found.
    # clang-tidy automatically searches for a .clang-tidy file in parent directories.
    add_custom_target(check-clang-tidy
            ${BUILD_SUPPORT_DIR}/run-clang-tidy.py                            # Run LLVM's clang-tidy script.
            -clang-tidy-binary ${CLANG_TIDY_BIN}                              # Using the specified clang-tidy binary.
            -p ${CMAKE_BINARY_DIR}                                            # Using the generated compile commands.
            )
    if (TERRIER_BUILD_BENCHMARKS)
        add_dependencies(check-clang-tidy benchmark gtest gflags)             # clang-tidy needs their headers to exist
    elseif (TERRIER_BUILD_TESTS)
        add_dependencies(check-clang-tidy gtest gflags)                       # clang-tidy needs their headers to exist
    endif ()
    message(STATUS "[ADDED] check-clang-tidy (${CLANG_TIDY_BIN})")
endif ()
unset(CLANG_TIDY_BIN)
