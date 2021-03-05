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
#
# Organization of this file. (You can Ctrl-F for these!)
#   HEADER Project definition.
#   HEADER Safety checks.
#   HEADER System info.
#   HEADER CMake options and global variables.
#   HEADER Dependencies for finding dependencies.
#   HEADER Dependencies.
#   HEADER noisepage libraries.
#   HEADER noisepage binary.
#   HEADER util_static and util_shared libraries.
#   HEADER hack_bytecode_handlers_ir target.
#   HEADER gen_opt_bc binary.
#   HEADER tpl binary.
#   HEADER Tests.
#   HEADER Benchmarks.
#   HEADER Generated file destinations.
#   HEADER Miscellaneous.

#######################################################################################################################
# HEADER Project definition.
#######################################################################################################################

# Ubuntu 20.04 ships with CMake version 3.16.3. But CI on Mac is outdated and picking up 3.15.
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
# HEADER Safety checks.
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
# HEADER System info.
#######################################################################################################################

# Print a welcome message with the project's version number.
message(STATUS
        "Welcome to NoisePage!\n\
        Home Page: ${PROJECT_HOMEPAGE_URL}\n\
        Version: ${PROJECT_VERSION}\n")

# CMAKE_MODULE_PATH is the search path for the include() and find_package() CMake commands.
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

message(STATUS "Compiler: ${CMAKE_CXX_COMPILER} ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "System: ${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_VERSION} ${CMAKE_SYSTEM_PROCESSOR}")

function(print_sys_info QUERY_TARGET)
    cmake_host_system_information(RESULT SYS_INFO QUERY ${QUERY_TARGET})
    message(STATUS "  System ${QUERY_TARGET}: ${SYS_INFO}")
endfunction()
print_sys_info("NUMBER_OF_LOGICAL_CORES;NUMBER_OF_PHYSICAL_CORES")
print_sys_info("HOSTNAME;FQDN")
print_sys_info("AVAILABLE_VIRTUAL_MEMORY;TOTAL_VIRTUAL_MEMORY")
print_sys_info("AVAILABLE_PHYSICAL_MEMORY;TOTAL_PHYSICAL_MEMORY")
print_sys_info("IS_64BIT;HAS_IA64")
print_sys_info("HAS_FPU;HAS_MMX;HAS_MMX_PLUS")
print_sys_info("HAS_SSE;HAS_SSE2;HAS_SSE_FP;HAS_SSE_MMX")
print_sys_info("HAS_AMD_3DNOW;HAS_AMD_3DNOW_PLUS")
print_sys_info("HAS_SERIAL_NUMBER;PROCESSOR_SERIAL_NUMBER")
print_sys_info("PROCESSOR_NAME;PROCESSOR_DESCRIPTION")
print_sys_info("OS_NAME;OS_RELEASE;OS_VERSION;OS_PLATFORM")

#######################################################################################################################
# HEADER CMake options and global variables.
# CMake build types, specify with -DCMAKE_BUILD_TYPE={option}.
#   Debug (default), Release, RelWithDebInfo, FastDebug.
#   In practice people only use Debug or Release.
#
# CMake options, specify with -DNOISEPAGE_{option}=On.
#   NOISEPAGE_BUILD_BENCHMARKS              : Enable building benchmarks as part of the ALL target. Default ON.
#   NOISEPAGE_BUILD_TESTS                   : Enable building tests as part of the ALL (but the Self-Driving test) target. Default ON.
#   NOISEPAGE_BUILD_SELF_DRIVING_E2E_TESTS  : Enable building self-driving end-to-end tests. Default OFF
#   NOISEPAGE_GENERATE_COVERAGE             : Enable C++ code coverage. Default OFF.
#   NOISEPAGE_UNITTEST_OUTPUT_ON_FAILURE    : Enable verbose unittest failures. Default OFF. Can be very verbose.
#   NOISEPAGE_UNITY_BUILD                   : Enable unity (aka jumbo) builds. Default OFF.
#   NOISEPAGE_USE_ASAN                      : Enable ASAN, a fast memory error detector. Default OFF.
#   NOISEPAGE_USE_JEMALLOC                  : Link with jemalloc instead of system malloc. Default OFF.
#   NOISEPAGE_USE_JUMBOTESTS                : Enable jumbotests instead of unittests as part of ALL target. Default OFF.
#   NOISEPAGE_USE_LOGGING                   : Enable logging. Default ON.
#
# CMake global variables. These are NOT CMake options, i.e., these variables are internal. Usually OS-specific hacks.
#   BUILD_SUPPORT_DIR             : Helper scripts for building belongs here.
#   BUILD_SUPPORT_DATA_DIR        : Helper data for building belongs here.
#   NOISEPAGE_ENABLE_SHARED       : On if we should enable shared targets and off otherwise.
#   NOISEPAGE_COMPILE_OPTIONS     : Compile options to be added to NoisePage.
#   NOISEPAGE_INCLUDE_DIRECTORIES : Include directories to be used for NoisePage.
#   NOISEPAGE_LINK_LIBRARIES      : Link libraries to be added to NoisePage.
#   NOISEPAGE_LINK_OPTIONS        : Link options to be added to NoisePage.
#   NOISEPAGE_TEST_PARALLELISM    : The number of tests that should run in parallel.
#######################################################################################################################

# Default to DEBUG builds if -DCMAKE_BUILD_TYPE was not specified.
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif (NOT CMAKE_BUILD_TYPE)

option(NOISEPAGE_BUILD_BENCHMARKS
        "Enable building benchmarks as part of the ALL target."
        ON)

option(NOISEPAGE_BUILD_TESTS
        "Enable building tests as part of the ALL target."
        ON)

option(NOISEPAGE_BUILD_SELF_DRIVING_E2E_TESTS
        "Enable building self-driving end-to-end tests as part of the ALL target."
        OFF)

option(NOISEPAGE_GENERATE_COVERAGE
        "Enable C++ code coverage."
        OFF)

set(NOISEPAGE_TEST_PARALLELISM
        "1"
        CACHE STRING "The maximum number of tests that can be run in parallel at a time. Warning: can cause weird bugs.")

option(NOISEPAGE_UNITTEST_OUTPUT_ON_FAILURE
        "Verbose output for unittests when they fail. Warning: on jumbo, this is VERY verbose!"
        OFF)

option(NOISEPAGE_UNITY_BUILD
        "Enable Unity builds for much faster compilation. https://cmake.org/cmake/help/latest/prop_tgt/UNITY_BUILD.html"
        OFF)

option(NOISEPAGE_USE_ASAN
        "Enable ASAN, a fast memory error detector. https://clang.llvm.org/docs/AddressSanitizer.html"
        OFF)

option(NOISEPAGE_USE_JEMALLOC
        "Link jemalloc instead of system malloc. https://github.com/jemalloc/jemalloc"
        OFF)

option(NOISEPAGE_USE_JUMBOTESTS
        "Enable jumbotests instead of unittest as part of ALL target."
        OFF)

option(NOISEPAGE_USE_LOGGING
        "Enable logging. When enabled, there is a performance hit for all logging calls even if nothing is logged."
        ON)

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")
set(BUILD_SUPPORT_DATA_DIR "${CMAKE_SOURCE_DIR}/build-support/data")

# Everything else in this section will populate the following global variables.
set(NOISEPAGE_COMPILE_OPTIONS "")
set(NOISEPAGE_LINK_LIBRARIES "")
set(NOISEPAGE_LINK_OPTIONS "")
set(NOISEPAGE_INCLUDE_DIRECTORIES "")

# Add compilation flags to NOISEPAGE_COMPILE_OPTIONS based on the current CMAKE_BUILD_TYPE.
string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-ggdb" "-O0" "-fno-omit-frame-pointer" "-fno-optimize-sibling-calls")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-ggdb" "-O1" "-fno-omit-frame-pointer" "-fno-optimize-sibling-calls")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    list(APPEND NOISEPAGE_COMPILE_DEFINITIONS "-DNDEBUG")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-O3")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELWITHDEBINFO")
    list(APPEND NOISEPAGE_COMPILE_DEFINITIONS "-DNDEBUG")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-ggdb" "-O2")
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
if (${NOISEPAGE_USE_ASAN})
    set(NOISEPAGE_ASAN_FLAGS
            "-fsanitize=address"                # Enable ASAN.
            "-fno-omit-frame-pointer"           # Nicer stack traces in error messages.
            "-fno-optimize-sibling-calls"       # Disable tail call elimination (perfect stack traces if inlining off).
            )
    list(APPEND NOISEPAGE_COMPILE_OPTIONS ${NOISEPAGE_ASAN_FLAGS})
    list(APPEND NOISEPAGE_LINK_OPTIONS "-fsanitize=address")
    unset(NOISEPAGE_ASAN_FLAGS)
    # Unfortunately, gcc does not support -fsanitize-blacklist. We ask users to set it manually as an env var.
endif ()
message(STATUS "ASAN: ${NOISEPAGE_USE_ASAN}")
unset(NOISEPAGE_ASAN_MSG)

# jemalloc.
set(NOISEPAGE_JEMALLOC_MSG "${NOISEPAGE_USE_JEMALLOC}")
if (${NOISEPAGE_USE_JEMALLOC})
    # We find jemalloc from the system to avoid building jemalloc from scratch.
    find_path(JEMALLOC_INCLUDE_DIR NAMES jemalloc/jemalloc.h REQUIRED)
    find_library(JEMALLOC_LIBRARIES NAMES jemalloc libjemalloc.so.1 libjemalloc.so.2 libjemalloc.dylib REQUIRED)
    list(APPEND NOISEPAGE_LINK_LIBRARIES ${JEMALLOC_LIBRARIES})         # Add to NoisePage link libs.
    list(APPEND NOISEPAGE_INCLUDE_DIRECTORIES ${JEMALLOC_INCLUDE_DIR})  # Add to NoisePage includes.
    unset(JEMALLOC_INCLUDE_DIR)                                         # Variable hygiene.
    unset(JEMALLOC_LIBRARIES)                                           # Variable hygiene.
    set(NOISEPAGE_JEMALLOC_MSG "On (dir:${JEMALLOC_INCLUDE_DIR} lib:${JEMALLOC_LIBRARIES})")
endif ()
message(STATUS "jemalloc: ${NOISEPAGE_JEMALLOC_MSG}")
unset(NOISEPAGE_JEMALLOC_MSG)

# spdlog.
if (${NOISEPAGE_USE_LOGGING})
    list(APPEND NOISEPAGE_COMPILE_DEFINITIONS "-DNOISEPAGE_USE_LOGGING")
endif ()
message(STATUS "Logging: ${NOISEPAGE_USE_LOGGING}")

message(STATUS "Verbose unit tests (NOISEPAGE_UNITTEST_OUTPUT_ON_FAILURE): ${NOISEPAGE_UNITTEST_OUTPUT_ON_FAILURE}")
message(STATUS "Unity builds (NOISEPAGE_UNITY_BUILD): ${NOISEPAGE_UNITY_BUILD}")
message(STATUS "Test max parallelism: ${NOISEPAGE_TEST_PARALLELISM} tests at a time.")

# OS specific configuration.
if (APPLE)
    # On OSX, clang complains about this.
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-Wno-braced-scalar-init")
    message(STATUS "OSX hack, NOISEPAGE_COMPILE_OPTIONS: adding -Wno-braced-scalar-init")
    # On OSX, using lld causes a linking error because -lz (zlib) cannot be found.
    message(STATUS "OSX hack, Linker: Will use default system linker.")
    # On OSX, LLVM is compiled with -fvisibility=hidden.
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-fvisibility=hidden")
    list(APPEND NOISEPAGE_LINK_OPTIONS "-fvisibility=hidden")
else ()
    # lld is the fastest linker.
    list(APPEND NOISEPAGE_LINK_OPTIONS "-fuse-ld=lld")
    message(STATUS "Linker: Will pass \"-fuse-ld=lld\" to linker.")
endif ()

# Compiler specific configuration.
# ASAN_OPTIONS=suppressions=... does not work because we compile third party libraries ourselves for the most part.
# Since the issues tend to manifest only on clang builds, we can use the clang-specific -fsanitize-blacklist= option.
# GCC devs refuse to add the option, so eh. Fingers crossed.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    message(STATUS "Clang hack, NOISEPAGE_COMPILE_OPTIONS: adding -fsanitize-blacklist=${BUILD_SUPPORT_DATA_DIR}/asan_sanitize_blocklist.txt.")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-fsanitize-blacklist=${BUILD_SUPPORT_DATA_DIR}/asan_sanitize_blocklist.txt")
endif ()

# The goal is to dynamically link tests to reduce filesize. However,
#   1. The OSX linker is a huge pain.
#   2. Clang has a bug with dynamically linked ASAN https://github.com/google/sanitizers/issues/1017
#      and the workaround no longer works.
#   3. Coverage builds are very slow and every little bit of speed helps.
# In those scenarios, we disable dynamic linking entirely and rely on static linking.
if (APPLE OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR ${NOISEPAGE_GENERATE_COVERAGE})
    set(NOISEPAGE_ENABLE_SHARED OFF)
    message(STATUS "Shared targets: Removed.")
    message(STATUS "Tests: Will use static linking.")
else ()
    set(NOISEPAGE_ENABLE_SHARED ON)
    message(STATUS "Shared targets: Available.")
    message(STATUS "Tests: Will use dynamic linking.")
endif ()

# From pmenon: since Ninja buffers output from the compiler, GCC/Clang won't generate colored text.
# Explicitly request colorized compiler output.
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-fdiagnostics-color=always")
    message(STATUS "Colorized output: ON (-fdiagnostics-color=always)")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    list(APPEND NOISEPAGE_COMPILE_OPTIONS "-fcolor-diagnostics")
    message(STATUS "Colorized output: ON (-fcolor-diagnostics)")
else ()
    message(STATUS "Colorized output: OFF (unknown compiler ${CMAKE_CXX_COMPILER_ID})")
endif ()

if ("${CMAKE_GENERATOR}" STREQUAL "Ninja")
    # Num Logical Cores = Compile Pool + Link Pool (1) + 1
    # The extra 1 at the end is there for good luck.
    # A pool controls the _maximum_ parallelism of the jobs assigned to the pool.

    # Force single-threaded linking because linking can consume a ton of memory.
    cmake_host_system_information(RESULT NINJA_COMPILE_POOL_SIZE QUERY "NUMBER_OF_LOGICAL_CORES")
    set(NINJA_LINK_POOL_SIZE 1)
    math(EXPR NINJA_COMPILE_POOL_SIZE "${NINJA_COMPILE_POOL_SIZE} - ${NINJA_LINK_POOL_SIZE} - 1")
    set_property(GLOBAL PROPERTY JOB_POOLS link_pool=${NINJA_LINK_POOL_SIZE} compile_pool=${NINJA_COMPILE_POOL_SIZE})
    # Set linking and compiling limits globally for all targets by modifying the default pool options.
    set(CMAKE_JOB_POOL_COMPILE compile_pool)
    set(CMAKE_JOB_POOL_LINK link_pool)
    message(STATUS "Ninja detected. Global pools: link=${NINJA_LINK_POOL_SIZE} compile=${NINJA_COMPILE_POOL_SIZE}")
endif()

#######################################################################################################################
# HEADER Dependencies for finding dependencies. : )
#   VARS_BEFORE() / VARS_AFTER()    : Useful during CMake development to see what new variables got added.
#   message()                       : Redefined to suppress informational messages when MESSAGE_QUIET is on.
#   MESSAGE_QUIET                   : Flag controlling message() behavior. ON to suppress informational messages.
#######################################################################################################################

set_property(GLOBAL PROPERTY CTEST_TARGETS_ADDED 1)     # Prevent CTest from adding targets later. A hack.

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

# Some libraries just won't shut up, so this redefines the message function.
set(MESSAGE_QUIET OFF)
function(message)
    list(GET ARGV 0 MessageType)
    if (NOT MESSAGE_QUIET OR
            MessageType STREQUAL FATAL_ERROR OR
            MessageType STREQUAL SEND_ERROR OR
            MessageType STREQUAL WARNING OR
            MessageType STREQUAL AUTHOR_WARNING)
        list(GET ARGV 1 Message)

        # Special-casing for messages to ignore.
        set(IGNORED_MESSAGES
                ""
                # Google Benchmark.
                "CMake's FindThreads.cmake did not fail, but CMAKE_THREAD_LIBS_INIT ended up being empty. This was fixed in https://github.com/Kitware/CMake/commit/d53317130e84898c5328c237186dbd995aaf1c12 Let's guess that -pthread is sufficient."
                )
        if (Message IN_LIST IGNORED_MESSAGES)
            return()
        endif ()

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
    set(DOWNLOAD_FOLDER ${CMAKE_BINARY_DIR}/_deps/src/${NAME})                          # Set download folder.
    if (EXISTS ${DOWNLOAD_FOLDER}/${FILE_NAME})                                         # Only download if required.
        message(STATUS "[FOUND] ${NAME} (already in ${DOWNLOAD_FOLDER}, skipping download)")
    else ()
        file(DOWNLOAD ${FILE_URL} ${DOWNLOAD_FOLDER}/${FILE_NAME})                      # Download the file.
        message(STATUS "[FOUND] ${NAME} (${FILE_URL})")                                 # Print a success message.
    endif ()
    set(${NAME_LOWER}_SOURCE_DIR ${DOWNLOAD_FOLDER} PARENT_SCOPE)                       # Set SOURCE_DIR in parent.
endfunction()

#######################################################################################################################
# HEADER Dependencies.
#######################################################################################################################

message(STATUS "\nDependencies: Locating...\n==========================================\n")

# DISGUSTING HACKS:
# - -fPIC is required on pretty much every dependency.
# - fsanitize=address is required on OSX.
set(OLD_CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}")     # Save the current CMAKE_BUILD_TYPE.
set(OLD_CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")           # Save the current CMAKE_C_FLAGS.
set(OLD_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")       # Save the current CMAKE_CXX_FLAGS.
set(CMAKE_BUILD_TYPE "Release")
set(CMAKE_C_FLAGS "-fPIC")
set(CMAKE_CXX_FLAGS "-fPIC")
if (${NOISEPAGE_USE_ASAN})
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif ()

message(STATUS "Third-party CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "Third-party CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
message(STATUS "Third-party CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

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

# ZeroMQ build settings.
set(CPPZMQ_BUILD_TESTS OFF CACHE INTERNAL "Disable building CPPZMQ tests.")

# Include the source of the dependencies as sources that NoisePage can include.
list(APPEND NOISEPAGE_INCLUDE_DIRECTORIES ${CMAKE_BINARY_DIR}/_deps/src/)

# Fetch single-file dependencies.
add_noisepage_dep_singlefile(csv https://raw.githubusercontent.com/vincentlaucsb/csv-parser/92694230ba4863a436b6533788e590fce70b5f44/single_include/csv.hpp)
add_noisepage_dep_singlefile(portable_endian https://gist.githubusercontent.com/panzi/6856583/raw/1eca2ab34f2301b9641aa73d1016b951fff3fc39/portable_endian.h)

# Fetch project dependencies.
add_noisepage_dep(count https://github.com/pmenon/libcount.git 6eef9d048d4577f144506ffc076c1913f8faf3ef)
add_noisepage_dep(cppzmq https://github.com/zeromq/cppzmq.git v4.7.1)
add_noisepage_dep(gflags https://github.com/gflags/gflags.git v2.2.2)
add_noisepage_dep(googlebenchmark https://github.com/google/benchmark.git v1.5.2)
add_noisepage_dep(googletest https://github.com/google/googletest.git release-1.10.0)
add_noisepage_dep(ips4o https://github.com/ips4o/ips4o.git 2fb65ca11ac1898faee2f146610e6409489d2105)
add_noisepage_dep(madoka https://github.com/s-yata/madoka.git 66783ee5b84a432f934517ad65452d54b19230bb)
add_noisepage_dep(nlohmann_json https://github.com/ArthurSonzogni/nlohmann_json_cmake_fetchcontent.git v3.7.3)
add_noisepage_dep(spdlog https://github.com/gabime/spdlog.git v1.8.1)
add_noisepage_dep(xbyak https://github.com/herumi/xbyak.git v5.77)
add_noisepage_dep(xxHash https://github.com/Cyan4973/xxHash.git v0.8.0)
add_noisepage_dep(fast_float https://github.com/lemire/fast_float.git v0.1.0)

# Handle other dependencies.

# TODO(WAN): libpg_query is CURSED. Someone else is welcome to fix it. Or I may retry in the future.
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/libpg_query/ EXCLUDE_FROM_ALL)

# libpqxx.
find_library(PQXX_LIBRARIES NAMES pqxx libpqxx REQUIRED)
find_path(PQXX_INCLUDE_DIRECTORIES NAMES pqxx/pqxx REQUIRED)
list(APPEND NOISEPAGE_LINK_LIBRARIES ${PQXX_LIBRARIES})
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
find_package(TBB REQUIRED)
message(STATUS "[FOUND] TBB ${TBB_VERSION}")

# DISGUSTING HACK: Restore the old CMAKE_BUILD_TYPE, CMAKE_C_FLAGS, and CMAKE_CXX_FLAGS.
set(CMAKE_BUILD_TYPE "${OLD_CMAKE_BUILD_TYPE}")     # Restore the old CMAKE_BUILD_TYPE.
set(CMAKE_C_FLAGS "${OLD_CMAKE_C_FLAGS}")           # Restore the old CMAKE_C_FLAGS.
set(CMAKE_CXX_FLAGS "${OLD_CMAKE_CXX_FLAGS}")       # Restore the old CMAKE_CXX_FLAGS.
unset(OLD_CMAKE_C_FLAGS)                            # Variable hygiene.
unset(OLD_CMAKE_CXX_FLAGS)                          # Variable hygiene.
unset(OLD_CMAKE_BUILD_TYPE)                         # Variable hygiene.

message(STATUS "\n==========================================\nDependencies: All found!\n")

#######################################################################################################################
# HEADER noisepage libraries.
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
list(REMOVE_ITEM NOISEPAGE_SRCS ${PROJECT_SOURCE_DIR}/src/main/noisepage.cpp)

# Build NoisePage as an OBJECT library first, i.e., a .o file per corresponding .cpp file.
# The OBJECT library is built first so that the same .o files can be linked into static and shared libraries.
# This allows both noisepage_static and noisepage_shared to be built with a single compilation of translation units.
add_library(noisepage_objlib OBJECT ${NOISEPAGE_SRCS})

set_target_properties(noisepage_objlib PROPERTIES
        POSITION_INDEPENDENT_CODE ON                # Required for static linking into other shared libraries.
        CXX_EXTENSIONS OFF                          # Disable compiler extensions (e.g., use c++17 not gnu17).
        UNITY_BUILD ${NOISEPAGE_UNITY_BUILD}        # Build multiple cpp files as a single cpp file.
        )
target_compile_definitions(noisepage_objlib PUBLIC  # PUBLIC: all consumers of the library inherit the following.
        ${NOISEPAGE_COMPILE_DEFINITIONS}
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
target_compile_features(noisepage_objlib PUBLIC     # PUBLIC: all consumers of the library inherit the following.
        cxx_std_17                                  # Require support for C++17.
        )
target_include_directories(noisepage_objlib PUBLIC  # PUBLIC: all consumers of the library inherit the following.
        ${PROJECT_SOURCE_DIR}/src/include           # Include NoisePage src/include/ headers.
        )
target_include_directories(noisepage_objlib SYSTEM PUBLIC   # SYSTEM PUBLIC: inherit without error checking.
        ${LLVM_INCLUDE_DIRS}                                # Include LLVM headers.
        ${NOISEPAGE_INCLUDE_DIRECTORIES}                    # Third-party includes.
        ${CMAKE_BINARY_DIR}/_deps/src/spdlog/include/       # Hack: spdlog.
        )
target_link_options(noisepage_objlib PUBLIC         # PUBLIC: all consumers of the library inherit the following.
        ${NOISEPAGE_LINK_OPTIONS}
        )

# Unfortunately, some libraries do not have clean target_link_libraries hygiene.
# The symptoms are typically weird -Werror warnings because the NoisePage Werror flags get applied to library code.
# The hacky solution is to manually specify their target_include_directories as SYSTEM.
target_include_directories(noisepage_objlib SYSTEM PUBLIC ${CMAKE_BINARY_DIR}/_deps/src/fast_float/include/)

target_link_libraries(noisepage_objlib PUBLIC       # PUBLIC: all consumers of the library inherit the following.
        count
        cppzmq
        fast_float
        gflags
        ips4o::ips4o
        madoka::madoka
        nlohmann_json::nlohmann_json
        pg_query::pg_query
        xbyak::xbyak
        xxHash::xxhash
        ${CMAKE_BINARY_DIR}/_deps/build/spdlog/libspdlog.a
        ${EVENT_LINK_LIBRARIES}
        ${EVENT_PTHREADS_LINK_LIBRARIES}
        ${NOISEPAGE_LINK_LIBRARIES}
        ${LLVM_LIBRARIES}
        ${TBB_LIBRARIES_RELEASE}
        )

# Create the noisepage_static and noisepage_shared libraries using the objects from noisepage_objlib.
add_library(noisepage_static STATIC $<TARGET_OBJECTS:noisepage_objlib>)     # Bundle up these objects into static lib.
target_link_libraries(noisepage_static PUBLIC noisepage_objlib)             # Consumers will inherit this link.
target_compile_options(noisepage_static PUBLIC      # PUBLIC: all consumers of the library inherit the following.
        "-fvisibility=hidden"                       # Hide symbols by default.
        )
target_link_options(noisepage_static PUBLIC         # PUBLIC: all consumers of the library inherit the following.
        "-fvisibility=hidden"                       # Hide symbols by default.
        )

# Dependencies are built in release because the debug versions have unacceptable performance for coverage builds.
# An example is spdlog. spdlog is not added as a true dependency because the library name changes between
# debug mode and release mode, namely libspdlogd.a versus libspdlog.a.
# Due to the different names, the build system (especially make) gets confused.
# Therefore the dependencies are manually built here.
# However, make is too stupid and ninja is too smart.
# make can't understand that TARGET DEPENDS on something.
# ninja looks for the files in DEPENDS and complains if those can't be found.
if (${CMAKE_GENERATOR} MATCHES "Unix Makefiles")
    add_dependencies(noisepage_static spdlog)
else()
    add_custom_command(TARGET noisepage_static DEPENDS spdlog)
endif()

if (${NOISEPAGE_ENABLE_SHARED})
    add_library(noisepage_shared SHARED $<TARGET_OBJECTS:noisepage_objlib>)   # Bundle up these objects into shared lib.
    target_link_libraries(noisepage_shared PUBLIC noisepage_objlib)           # Consumers will inherit this link.
endif ()

#######################################################################################################################
# HEADER noisepage binary.
# noisepage             :   The main DBMS binary.
#######################################################################################################################

add_executable(noisepage src/main/noisepage.cpp)
target_compile_options(noisepage PRIVATE "-Werror" "-Wall")
target_link_libraries(noisepage noisepage_static)
set_target_properties(noisepage PROPERTIES CXX_EXTENSIONS OFF ENABLE_EXPORTS ON)

#######################################################################################################################
# HEADER util_static and util_shared libraries.
# util_static           :   table_generator and table_reader for tests and benchmarks.
# util_shared           :   table_generator and table_reader for tests and benchmarks.
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

function(add_util_lib TYPE)
    string(TOLOWER ${TYPE} TYPE_LOWER)
    add_library(util_${TYPE_LOWER} ${TYPE} ${UTIL_SRCS})
    target_compile_options(util_${TYPE_LOWER} PRIVATE "-Werror" "-Wall")
    target_include_directories(util_${TYPE_LOWER} PUBLIC ${PROJECT_SOURCE_DIR}/util/include/)
    target_link_libraries(util_${TYPE_LOWER} PUBLIC noisepage_${TYPE_LOWER})
    set_target_properties(util_${TYPE_LOWER} PROPERTIES CXX_EXTENSIONS OFF UNITY_BUILD ${NOISEPAGE_UNITY_BUILD})
endfunction()

add_util_lib(STATIC)
if (${NOISEPAGE_ENABLE_SHARED})
    add_util_lib(SHARED)
endif ()

#######################################################################################################################
# HEADER hack_bytecode_handlers_ir target.
# hack_bytecode_handlers_ir     :   This target is not actually used to build the bytecode handlers, because the
#                                   bytecode handlers have to be built with clang and it is possible that the current
#                                   compiler is gcc -- you simply cannot mix two compilers in one CMake project
#                                   without jumping through SuperProject hoops. The target is created to get the
#                                   relevant entry in compile_commands.json, which is then parsed and run manually
#                                   with clang... :(
#######################################################################################################################

add_library(hack_bytecode_handlers_ir EXCLUDE_FROM_ALL util/execution/bytecode_handlers_ir.cpp)
target_link_libraries(hack_bytecode_handlers_ir PRIVATE noisepage_static)
set_target_properties(hack_bytecode_handlers_ir PROPERTIES CXX_EXTENSIONS OFF)

# On MacOS, the clang++ we want is not the clang++ in PATH. On Linux, it is.
# NOTE(Kyle): It is important that CLANG is resolved prior to the commands
# below, so the location of the following definition matters.
if (APPLE)
    set(CLANG "${LLVM_TOOLS_BINARY_DIR}/clang++")
else ()
    find_program(CLANG NAMES "clang++-8" "clang++" REQUIRED)
endif ()

# Run the build-support script that generates and optimizes bitcode.
add_custom_command(
        OUTPUT "${CMAKE_BINARY_DIR}/bin/bytecode_handlers_ir.bc"
        DEPENDS gen_opt_bc "${PROJECT_SOURCE_DIR}/src/include/execution/vm/bytecodes.h" "${PROJECT_SOURCE_DIR}/src/include/execution/vm/bytecode_handlers.h"
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMAND ${BUILD_SUPPORT_DIR}/generate-bitcodes.sh ${PROJECT_SOURCE_DIR} ${CMAKE_BINARY_DIR} ${CLANG}
)

# Here, we create a custom target that merely wraps the output file
# from the custom command above; this ensures that the custom command
# is actually only ever invoked once even when multiple indepdendent
# targets that require bitcode are built because the build system 
# (e.g. Make, Ninja) is now 'aware' of the target-level dependency.
add_custom_target(bitcode_generator DEPENDS "${CMAKE_BINARY_DIR}/bin/bytecode_handlers_ir.bc")

#######################################################################################################################
# HEADER gen_opt_bc binary.
# gen_opt_bc            :   Generates optimized bitcode from the LLVM IR file generated as a tpl post-build target.
#######################################################################################################################

add_executable(gen_opt_bc util/execution/gen_opt_bc.cpp)
target_compile_options(gen_opt_bc PRIVATE "-Werror" "-Wall")
target_include_directories(gen_opt_bc PRIVATE ${LLVM_INCLUDE_DIRS})
target_link_libraries(gen_opt_bc PRIVATE ${LLVM_LIBRARIES})
set_target_properties(gen_opt_bc PROPERTIES CXX_EXTENSIONS OFF ENABLE_EXPORTS ON)

add_dependencies(noisepage_static bitcode_generator)

#######################################################################################################################
# HEADER tpl binary.
# tpl                   :   Standalone binary for testing .tpl files and commands.
#                           Also generates POST_BUILD bytecode_handlers_ir.bc.
#######################################################################################################################

add_executable(tpl util/execution/tpl.cpp)
add_dependencies(tpl bitcode_generator)
target_compile_options(tpl PRIVATE "-Werror" "-Wall")
target_link_libraries(tpl PUBLIC noisepage_static util_static)
set_target_properties(tpl PROPERTIES CXX_EXTENSIONS OFF ENABLE_EXPORTS ON)

#######################################################################################################################
# HEADER Tests.
#######################################################################################################################

include(CTest)          # CTest support is built into CMake.
enable_testing()        # CTest support is built into CMake.
include(GoogleTest)     # We rely on GoogleTest for all our tests.
configure_file(test/CTestCustom.cmake ${PROJECT_BINARY_DIR}/CTestCustom.cmake COPYONLY) # Configure CTest output.

set(UNITTEST_OUTPUT_ON_FAILURE "")
if (${NOISEPAGE_UNITTEST_OUTPUT_ON_FAILURE})
    set(UNITTEST_OUTPUT_ON_FAILURE "--output-on-failure")
endif ()

# unittest and jumbotests invoke ctest on tests which are labeled "unittest" and "jumbotests" respectively.
# Unit tests are executed in parallel because we only care about correctness, not performance.
# This means that two different unit tests could be executing at the same time.
# In practice, figuring out the number to -j seems annoying and there doesn't appear to be a penalty for having too
# many tests queued up, so we'll use the magic number ${NOISEPAGE_TEST_PARALLELISM}.
# Footgun: Running tests in parallel causes wonderful breakages on OSX/clang. Not on Ubuntu/clang though.
# Footgun: labels passed to -L are regex. Give them very unique names.
add_custom_target(unittest
        ctest
        --resource-spec-file ${BUILD_SUPPORT_DATA_DIR}/ctest_resource_specs.json    # For controlling conflicting tests.
        ${UNITTEST_OUTPUT_ON_FAILURE}       # Whether to print output when a test fails.
        -j ${NOISEPAGE_TEST_PARALLELISM}    # Maximum number of parallel jobs.
        -L unittest                         # Run all tests that have a label like unittests. See footgun warning above.
        --no-compress-output                # Output verbosely so that it can be logged.
        -T Test                             # Run tests and log it to Testing/*/Test.xml.
        --timeout 3000                      # 3000 second timeout per test.
        USES_TERMINAL)
add_custom_target(jumbotests
        ctest
        --resource-spec-file ${BUILD_SUPPORT_DATA_DIR}/ctest_resource_specs.json    # For controlling conflicting tests.
        ${UNITTEST_OUTPUT_ON_FAILURE}       # Whether to print output when a test fails -- warning, will be VERBOSE.
        -j ${NOISEPAGE_TEST_PARALLELISM}    # Maximum number of parallel jobs.
        -L jumbotests                       # Run all tests that have a label like jumbotests. See footgun warning above.
        --no-compress-output                # Output verbosely so that it can be logged.
        -T Test                             # Run tests and log it to Testing/*/Test.xml.
        --timeout 3000                      # 3000 second timeout per test.
        USES_TERMINAL)
add_custom_target(self_driving_e2e_test         # For now, this target is specifically used for self-driving pipeline
        ctest
        --resource-spec-file ${BUILD_SUPPORT_DATA_DIR}/ctest_resource_specs.json    # For controlling conflicting tests.
        ${UNITTEST_OUTPUT_ON_FAILURE}       # Whether to print output when a test fails.
        -j ${NOISEPAGE_TEST_PARALLELISM}    # Maximum number of parallel jobs.
        -L self_driving_e2e_test                # Run all tests that have a label like this. See footgun warning above.
        --no-compress-output                # Output verbosely so that it can be logged.
        -T Test                             # Run tests and log it to Testing/*/Test.xml.
        --timeout 3000                      # 3000 second timeout per test.
        USES_TERMINAL)

unset(UNITTEST_OUTPUT_ON_FAILURE)

file(GLOB_RECURSE
        NOISEPAGE_TEST_UTIL_SRCS
        CONFIGURE_DEPENDS
        ${PROJECT_SOURCE_DIR}/test/include/test_util/*.h
        ${PROJECT_SOURCE_DIR}/test/test_util/*.cpp
        )

function(add_test_util_lib TYPE)
    string(TOLOWER ${TYPE} TYPE_LOWER)
    add_library(noisepage_test_util_${TYPE_LOWER} ${TYPE} ${NOISEPAGE_TEST_UTIL_SRCS})
    add_custom_command(TARGET noisepage_test_util_${TYPE_LOWER} DEPENDS gtest gtest_main gmock gmock_main)
    target_compile_options(noisepage_test_util_${TYPE_LOWER} PRIVATE "-Werror" "-Wall")
    # Inject the source directory path into the translation units for test utility lib
    target_compile_definitions(noisepage_test_util_${TYPE_LOWER} PRIVATE NOISEPAGE_BUILD_ROOT=${CMAKE_BINARY_DIR})
    target_include_directories(noisepage_test_util_${TYPE_LOWER} PUBLIC ${PROJECT_SOURCE_DIR}/test/include/)
    target_include_directories(noisepage_test_util_${TYPE_LOWER} SYSTEM PUBLIC
            ${CMAKE_BINARY_DIR}/_deps/src/googletest/googlemock/include/
            ${CMAKE_BINARY_DIR}/_deps/src/googletest/googletest/include/
            )
    target_link_libraries(noisepage_test_util_${TYPE_LOWER} PUBLIC
            ${CMAKE_BINARY_DIR}/lib/libgtest.a ${CMAKE_BINARY_DIR}/lib/libgmock.a
            util_${TYPE_LOWER} pqxx)
    set_target_properties(noisepage_test_util_${TYPE_LOWER} PROPERTIES CXX_EXTENSIONS OFF UNITY_BUILD ${NOISEPAGE_UNITY_BUILD})
endfunction()

add_test_util_lib(STATIC)
if (${NOISEPAGE_ENABLE_SHARED})
    add_test_util_lib(SHARED)
endif ()

function(add_noisepage_test
        TEST_NAME                   # The name of this test.
        TEST_SOURCES                # The CPP files for this test.
        TEST_LABEL                  # The label of this test. Will be added as a dependency of this label.
        SHOULD_EXCLUDE_FROM_ALL     # EXCLUDE_ALL if we should exclude from default ALL target, NO_EXCLUDE otherwise.
        SHOULD_UNITY_BUILD          # UNITY if we should unity build, NO_UNITY otherwise.
        REQUIRES_BITCODE            # BITCODE if this test requires bitcodes to be generated, NO_BITCODE otherwise.
        )
    set(TEST_OUTPUT_DIR "${CMAKE_BINARY_DIR}/test")             # Output directory for tests.

    if (${SHOULD_EXCLUDE_FROM_ALL} STREQUAL "EXCLUDE_ALL")
        set(EXCLUDE_OPTION "EXCLUDE_FROM_ALL")
    elseif (${SHOULD_EXCLUDE_FROM_ALL} STREQUAL "NO_EXCLUDE")
        set(EXCLUDE_OPTION "")
    else ()
        message(FATAL_ERROR "Invalid option for SHOULD_EXCLUDE_FROM_ALL.")
    endif ()

    if (${SHOULD_UNITY_BUILD} STREQUAL "UNITY")
        set(UNITY_OPTION "ON")
    elseif (${SHOULD_UNITY_BUILD} STREQUAL "NO_UNITY")
        set(UNITY_OPTION "OFF")
    else ()
        message(FATAL_ERROR "Invalid option for SHOULD_UNITY_BUILD.")
    endif ()

    add_executable(${TEST_NAME} ${EXCLUDE_OPTION} ${TEST_SOURCES})

    target_compile_options(${TEST_NAME} PRIVATE "-Werror" "-Wall" "-fvisibility=hidden")
    target_link_libraries(${TEST_NAME} PRIVATE ${CMAKE_BINARY_DIR}/lib/libgmock_main.a)
    if (${NOISEPAGE_ENABLE_SHARED})
        target_link_libraries(${TEST_NAME} PRIVATE noisepage_test_util_shared)
    else ()
        target_link_libraries(${TEST_NAME} PRIVATE noisepage_test_util_static)
    endif ()

    if (${REQUIRES_BITCODE} STREQUAL "BITCODE")
        add_dependencies(${TEST_NAME} bitcode_generator)
    endif()

    set_target_properties(${TEST_NAME} PROPERTIES
            CXX_EXTENSIONS OFF                                  # Disable compiler-specific extensions.
            ENABLE_EXPORTS ON                                   # Export for tpl.
            RUNTIME_OUTPUT_DIRECTORY "${TEST_OUTPUT_DIR}"       # Output the test binaries to this folder.
            UNITY_BUILD "${UNITY_OPTION}"                       # Possibly choose to use unity builds.
            )
    # Include the testing directories.
    target_include_directories(${TEST_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/test/include/)
    # TODO(WAN): The "modern" gtest_discover_test has a ton of files. Favoring legacy add_test for now...
    add_test(${TEST_NAME} ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} test ${TEST_OUTPUT_DIR}/${TEST_NAME})
    # Label each test with TEST_LABEL so that ctest can run all the tests under the TEST_LABEL label later.
    set_tests_properties(${TEST_NAME} PROPERTIES LABELS "${TEST_LABEL};${TEST_NAME}")
    # Add TEST_NAME as a dependency to TEST_LABEL. Note that TEST_LABEL must be a valid target!
    add_dependencies(${TEST_LABEL} ${TEST_NAME})
endfunction()

# NOTE:Self-driving End-To-End tests are not included below because the test require dependencies to other part of
# the system, thus could not be run as standalone tests.  They will be added to a separate test target:
# self_driving_e2e_test. See NOISEPAGE_SELF_DRIVING_E2E_TEST_SOURCES for details.
file(GLOB_RECURSE NOISEPAGE_TEST_SOURCES
        "test/binder/*.cpp"
        "test/catalog/*.cpp"
        "test/common/*.cpp"
        "test/execution/*.cpp"
        "test/integration/*.cpp"
        "test/messenger/*.cpp"
        "test/metrics/*.cpp"
        "test/network/*.cpp"
        "test/optimizer/*.cpp"
        "test/parser/*.cpp"
        "test/planner/*.cpp"
        "test/self_driving/*.cpp"
        "test/settings/*.cpp"
        "test/storage/*.cpp"
        "test/traffic_cop/*.cpp"
        "test/transaction/*.cpp"
        "test/type/*.cpp"
        )

# The individual tests that require bytecode_handlers_ir.bc present in the test directory.
set(TESTS_REQUIRING_BITCODE atomics_test)

foreach (NOISEPAGE_TEST_CPP ${NOISEPAGE_TEST_SOURCES})
    file(RELATIVE_PATH NOISEPAGE_TEST_CPP_REL "${PROJECT_SOURCE_DIR}/test" ${NOISEPAGE_TEST_CPP})
    get_filename_component(NOISEPAGE_TEST_DIR ${NOISEPAGE_TEST_CPP_REL} DIRECTORY)
    get_filename_component(NOISEPAGE_TEST ${NOISEPAGE_TEST_CPP} NAME_WE)

    if (NOT ${NOISEPAGE_BUILD_TESTS})
        set(EXCLUDE_OR_NOT "EXCLUDE_ALL")
    elseif (${NOISEPAGE_USE_JUMBOTESTS})
        set(EXCLUDE_OR_NOT "EXCLUDE_ALL")
    else ()
        set(EXCLUDE_OR_NOT "NO_EXCLUDE")
    endif ()

    if (${NOISEPAGE_TEST} IN_LIST TESTS_REQUIRING_BITCODE) 
        set(TEST_REQUIRES_BITCODE "BITCODE")
    else ()
        set(TEST_REQUIRES_BITCODE "NO_BITCODE")
    endif ()

    add_noisepage_test(${NOISEPAGE_TEST} ${NOISEPAGE_TEST_CPP} unittest ${EXCLUDE_OR_NOT} NO_UNITY ${TEST_REQUIRES_BITCODE})
endforeach ()

file(GLOB_RECURSE NOISEPAGE_SELF_DRIVING_E2E_TEST_SOURCES
        "test/self_driving_e2e/*.cpp"
        )

foreach (NOISEPAGE_TEST_CPP ${NOISEPAGE_SELF_DRIVING_E2E_TEST_SOURCES})
    file(RELATIVE_PATH NOISEPAGE_TEST_CPP_REL "${PROJECT_SOURCE_DIR}/test" ${NOISEPAGE_TEST_CPP})
    get_filename_component(NOISEPAGE_TEST_DIR ${NOISEPAGE_TEST_CPP_REL} DIRECTORY)
    get_filename_component(NOISEPAGE_TEST ${NOISEPAGE_TEST_CPP} NAME_WE)

    if (NOT ${NOISEPAGE_BUILD_SELF_DRIVING_E2E_TESTS})
        set(EXCLUDE_OR_NOT "EXCLUDE_ALL")
    else ()
        set(EXCLUDE_OR_NOT "NO_EXCLUDE")
    endif ()

    add_noisepage_test(${NOISEPAGE_TEST} ${NOISEPAGE_TEST_CPP} self_driving_e2e_test ${EXCLUDE_OR_NOT} NO_UNITY FALSE)
endforeach ()

# The jumbotest for specific prefixes that require bitcode. 
set(PREFIXES_REQUIRING_BITCODE "")
# The jumbotests for entire folders that require bitcode.
set(FOLDERS_REQUIRING_BITCODE execution)

function(add_jumbotest
        FOLDER_PATH     # The folder to jumbo together.
        PREFIXES        # The prefixes of filenames within that folder that should be separate jumbos of their own.
        )
    set(TEST_OUTPUT_DIR "${CMAKE_BINARY_DIR}/test")             # Output directory for tests.
    get_filename_component(FOLDER_NAME ${FOLDER_PATH} NAME_WE)
    set(TEST_NAME jumbotest_${FOLDER_NAME})
    file(GLOB_RECURSE JUMBO_SOURCES_ALL ${FOLDER_PATH}/*.cpp)
    set(TEST_NAME_SUFFIX "")

    if (NOT ${NOISEPAGE_BUILD_TESTS})
        set(EXCLUDE_OR_NOT "EXCLUDE_ALL")
    elseif (${NOISEPAGE_USE_JUMBOTESTS})
        set(EXCLUDE_OR_NOT "NO_EXCLUDE")
    else ()
        set(EXCLUDE_OR_NOT "EXCLUDE_ALL")
    endif ()

    if (${NOISEPAGE_UNITY_BUILD})
        set(UNITY_OR_NOT "UNITY")
    else ()
        set(UNITY_OR_NOT "NO_UNITY")
    endif ()

    if (NOT "${PREFIXES}" STREQUAL "")
        foreach (PREFIX ${PREFIXES})
            file(GLOB_RECURSE JUMBO_SOURCES ${FOLDER_PATH}/${PREFIX}*.cpp)
            foreach (PREFIXED_FILE ${JUMBO_SOURCES})
                list(REMOVE_ITEM JUMBO_SOURCES_ALL ${PREFIXED_FILE})
            endforeach ()

            if (${PREFIX} IN_LIST PREFIXES_REQUIRING_BITCODE) 
                set(TEST_REQUIRES_BITCODE "BITCODE")
            else ()
                set(TEST_REQUIRES_BITCODE "NO_BITCODE")
            endif ()

            add_noisepage_test(jumbotest_${FOLDER_NAME}_${PREFIX} "${JUMBO_SOURCES}" "jumbotests" ${EXCLUDE_OR_NOT} ${UNITY_OR_NOT} ${TEST_REQUIRES_BITCODE})
        endforeach ()
        set(TEST_NAME_SUFFIX "_other")
    endif ()
    if (NOT "${JUMBO_SOURCES_ALL}" STREQUAL "")
        if (${FOLDER_NAME} IN_LIST FOLDERS_REQUIRING_BITCODE) 
            set(TEST_REQUIRES_BITCODE "BITCODE")
        else ()
            set(TEST_REQUIRES_BITCODE "NO_BITCODE")
        endif ()
        add_noisepage_test(jumbotest_${FOLDER_NAME}${TEST_NAME_SUFFIX} "${JUMBO_SOURCES_ALL}" "jumbotests" ${EXCLUDE_OR_NOT} ${UNITY_OR_NOT} ${TEST_REQUIRES_BITCODE})
    endif ()
endfunction()

# Tests are split up for the following reasons:
#   1. Faster compilation times - the fewer the overall test targets, the faster it compiles.
#   2. Faster test execution times - the long running tests are split off on their own.
#   3. Resource sharing - if two tests both want port 15721, then only one of them can run at a time.
# If you compiled regular non-jumbo tests, you would get 2 and 3 automatically. But 1 suffers, hence jumbotests.
#
# The second argument to add_jumbotest is a CMake list (aka a ;-separated string) of unique test prefixes.
# Each prefix will be globbed together into one test executable.
# Be careful that your prefixes are really unique, otherwise a test will be run multiple times.

add_jumbotest("test/binder" "")
add_jumbotest("test/catalog" "catalog_sql_test;")
add_jumbotest("test/common" "bitmap_test;concurrent_bitmap_test;rusage_monitor_test;")
add_jumbotest("test/execution" "ast;compiler_test;index_create_test;index_iterator_test;sql;storage_interface_test;util;vm;")
add_jumbotest("test/integration" "")
add_jumbotest("test/messenger" "messenger_test;")
add_jumbotest("test/metrics" "metrics_test;")
add_jumbotest("test/network" "network_test;")
add_jumbotest("test/optimizer" "hyperloglog_test;")
add_jumbotest("test/parser" "")
add_jumbotest("test/planner" "")
add_jumbotest("test/self_driving" "")
add_jumbotest("test/settings" "")
add_jumbotest("test/storage" "block_access_controller_test;block_compactor_test;bwtree_test;bwtree_index_test;data_table_test;data_table_concurrent_test;hash_index_test;large_garbage_collector_test;log_test;tuple_access_strategy_test;")
add_jumbotest("test/traffic_cop" "traffic_cop_test;")
add_jumbotest("test/transaction" "large_transaction_test;")
add_jumbotest("test/type" "")

# Read up on CTest resource groups: https://cmake.org/cmake/help/latest/manual/ctest.1.html#resource-allocation
# All tests that try to launch the server on port 15721 must declare that here.
set_tests_properties(
        catalog_sql_test jumbotest_catalog_catalog_sql_test
        metrics_test jumbotest_metrics_metrics_test
        network_test jumbotest_network_network_test
        traffic_cop_test jumbotest_traffic_cop_traffic_cop_test
        PROPERTIES RESOURCE_GROUPS "port15721:1")
set_tests_properties(
        messenger_test jumbotest_messenger_messenger_test
        PROPERTIES RESOURCE_GROUPS "port15721:1;port15722:1;port15723:1;port9022:1;port9023:1;port9024:1")
set_tests_properties(
        model_server_test
        PROPERTIES RESOURCE_GROUPS "port15721:1;port9022:1")
#######################################################################################################################
# HEADER Benchmarks.
#######################################################################################################################

file(GLOB_RECURSE
        NOISEPAGE_BENCHMARK_UTIL_SRCS
        CONFIGURE_DEPENDS
        ${PROJECT_SOURCE_DIR}/benchmark/include/benchmark_util/*.h
        ${PROJECT_SOURCE_DIR}/benchmark/benchmark_util/*.cpp
        )

add_library(noisepage_benchmark_util STATIC ${NOISEPAGE_BENCHMARK_UTIL_SRCS})
add_custom_command(TARGET noisepage_benchmark_util DEPENDS gtest gtest_main gmock gmock_main)
target_compile_options(noisepage_benchmark_util PRIVATE "-Werror" "-Wall")
target_include_directories(noisepage_benchmark_util PUBLIC ${PROJECT_SOURCE_DIR}/benchmark/include)
target_link_libraries(noisepage_benchmark_util PUBLIC ${CMAKE_BINARY_DIR}/lib/libgmock_main.a noisepage_test_util_static benchmark)
set_target_properties(noisepage_benchmark_util PROPERTIES CXX_EXTENSIONS OFF)

set(NOISEPAGE_BENCHMARKS "")
function(add_noisepage_benchmark BENCHMARK_NAME BENCHMARK_CPP)
    if (${NOISEPAGE_BUILD_BENCHMARKS})
        add_executable(${BENCHMARK_NAME} ${BENCHMARK_CPP})
    else ()
        add_executable(${BENCHMARK_NAME} EXCLUDE_FROM_ALL ${BENCHMARK_CPP})
    endif ()

    target_compile_options(${BENCHMARK_NAME} PRIVATE "-Werror" "-Wall")
    target_include_directories(${BENCHMARK_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/benchmark/include/)
    target_link_libraries(${BENCHMARK_NAME} PRIVATE noisepage_benchmark_util ${ARGN})
    target_link_directories(${BENCHMARK_NAME} PRIVATE ${CMAKE_BINARY_DIR})
    set_target_properties(${BENCHMARK_NAME} PROPERTIES
            CXX_EXTENSIONS OFF
            ENABLE_EXPORTS ON
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark"
            )

    set(BENCHMARK_PATH "${CMAKE_BINARY_DIR}/benchmark/${BENCHMARK_NAME}")
    add_test(${BENCHMARK_NAME}
            ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} benchmark ${BENCHMARK_PATH})
    set_tests_properties(${BENCHMARK_NAME} PROPERTIES LABELS "benchmark")
    list(APPEND NOISEPAGE_BENCHMARK ${BENCHMARK_NAME})
endfunction()

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

add_custom_target(runbenchmark ctest -L benchmark --no-compress-output -T Test USES_TERMINAL)
foreach (NOISEPAGE_BENCHMARK_CPP ${NOISEPAGE_BENCHMARK_SOURCES})
    get_filename_component(NOISEPAGE_BENCHMARK ${NOISEPAGE_BENCHMARK_CPP} NAME_WE)
    add_noisepage_benchmark(${NOISEPAGE_BENCHMARK} ${NOISEPAGE_BENCHMARK_CPP})
    add_dependencies(runbenchmark ${NOISEPAGE_BENCHMARK})
endforeach ()

#######################################################################################################################
# HEADER Generated file destinations.
#######################################################################################################################

set_target_properties(
        noisepage_static
        util_static
        noisepage_test_util_static
        noisepage_benchmark_util
        gen_opt_bc tpl noisepage
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

if (${NOISEPAGE_ENABLE_SHARED})
    set_target_properties(
            noisepage_shared
            util_shared
            noisepage_test_util_shared
            PROPERTIES
            ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
            LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
    )
endif ()

#######################################################################################################################
# HEADER Miscellaneous.
# This is where helper scripts and other random CMake stuff goes.
#######################################################################################################################

function(print_property TARGET_NAME PROPERTY_NAME)
    get_target_property(TARGET_NAME_PROPERTY ${TARGET_NAME} ${PROPERTY_NAME})
    if (NOT (TARGET_NAME_PROPERTY STREQUAL "TARGET_NAME_PROPERTY-NOTFOUND"))
        message(STATUS "    ${TARGET_NAME} ${PROPERTY_NAME}: ${TARGET_NAME_PROPERTY}")
    endif ()
endfunction()

function(print_properties TARGET_NAME)
    print_property(${TARGET_NAME} COMPILE_DEFINITIONS)
    print_property(${TARGET_NAME} COMPILE_FEATURES)
    print_property(${TARGET_NAME} COMPILE_OPTIONS)
    print_property(${TARGET_NAME} LINK_OPTIONS)
    print_property(${TARGET_NAME} LINK_LIBRARIES)
endfunction()

message(STATUS "Note that following NoisePage options are just a sample and may not be the full list. Check compile_commands.json to be sure.")

print_properties(noisepage_objlib)
print_properties(noisepage)
print_properties(tpl)
print_properties(noisepage_test_util_static)
print_properties(tpcc_test)
print_properties(noisepage_benchmark_util)
print_properties(tpcc_benchmark)

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
# CLANG_TOOLS_SEARCH_PATH   :   Where to look for clang programs like clang-format and clang-tidy.
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

set(CLANG_TOOLS_SEARCH_PATH
        "/usr/local/bin"
        "/usr/bin"
        "/usr/local/opt/llvm/bin"
        "/usr/local/opt/llvm@8/bin"
        "/usr/local/Cellar/llvm/8.0.1/bin"
        )

#######################################################################################################################
# check-censored    :   Grep the repository for some bad keywords. Brittle, but cheap and useful.
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
# check-lint        :   Run the cpplint python script.
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
# check-tpl         :   Python script to check core TPL functionality.
#######################################################################################################################

if (${NOISEPAGE_USE_LOGGING})
    add_custom_target(
            check-tpl
            DEPENDS tpl
            COMMAND cmake -E echo "Running: ${BUILD_SUPPORT_DIR}/run_tpl_tests.py -b ${CMAKE_BINARY_DIR}/bin/tpl -f ${PROJECT_SOURCE_DIR}/sample_tpl/tpl_tests.txt -t ${PROJECT_SOURCE_DIR}/sample_tpl -d ${CMAKE_BINARY_DIR}/bin"
            COMMAND ${BUILD_SUPPORT_DIR}/run_tpl_tests.py
            -b ${CMAKE_BINARY_DIR}/bin/tpl
            -f ${PROJECT_SOURCE_DIR}/sample_tpl/tpl_tests.txt
            -t ${PROJECT_SOURCE_DIR}/sample_tpl
            -d ${CMAKE_BINARY_DIR}/bin
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
            USES_TERMINAL
    )
    message(STATUS "[ADDED] check-tpl")
else ()
    message(STATUS "[MISSING] check-tpl does not work unless NOISEPAGE_USE_LOGGING is provided.")
endif ()

#######################################################################################################################
# format            :   Reformat the codebase according to standards.
# check-format      :   Check if the codebase is formatted according to standards.
#######################################################################################################################

find_program(CLANG_FORMAT_BIN NAMES clang-format-8 clang-format HINTS ${CLANG_TOOLS_SEARCH_PATH})
if ("${CLANG_FORMAT_BIN}" STREQUAL "CLANG_FORMAT_BIN-NOTFOUND")
    message(STATUS "[MISSING] clang-format not found, no format and no check-format.")
else ()
    # The directories to be formatted. Note that we modified the format script to take in multiple arguments.
    string(CONCAT FORMAT_DIRS
            "${CMAKE_CURRENT_SOURCE_DIR}/benchmark,"
            "${CMAKE_CURRENT_SOURCE_DIR}/src,"
            "${CMAKE_CURRENT_SOURCE_DIR}/test,"
            "${CMAKE_CURRENT_SOURCE_DIR}/util"
            )

    # Run clang-format and update files in place.
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

    # Run clang-format and exit with a non-zero exit code if any files need to be reformatted.
    add_custom_target(check-format
            ${BUILD_SUPPORT_DIR}/run_clang_format.py
            ${CLANG_FORMAT_BIN}
            ${BUILD_SUPPORT_DATA_DIR}/clangformat_suppressions.txt
            --source_dirs
            ${FORMAT_DIRS}
            --quiet
            USES_TERMINAL
            )

    message(STATUS "[ADDED] clang-format and check-clang-format (${CLANG_FORMAT_BIN})")

    unset(FORMAT_DIRS)
endif ()
unset(CLANG_FORMAT_BIN)

#######################################################################################################################
# check-clang-tidy  :   Run clang-tidy static analysis on the codebase.
#######################################################################################################################

find_program(CLANG_TIDY_BIN NAMES clang-tidy-8 clang-tidy HINTS ${CLANG_TOOLS_SEARCH_PATH})
if ("${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND")
    message(STATUS "[MISSING] clang-tidy not found, no check-clang-tidy.")
else ()
    # Run clang-tidy and exit with a non-zero exit code if any errors are found.
    # clang-tidy automatically searches for a .clang-tidy file in parent directories.
    add_custom_target(check-clang-tidy
            ${BUILD_SUPPORT_DIR}/run-clang-tidy.py                            # Run LLVM's clang-tidy script.
            -clang-tidy-binary ${CLANG_TIDY_BIN}                              # Using the specified clang-tidy binary.
            -p ${CMAKE_BINARY_DIR}                                            # Using the generated compile commands.
            USES_TERMINAL
            )
    add_custom_command(TARGET check-clang-tidy DEPENDS benchmark gflags gtest)
    message(STATUS "[ADDED] check-clang-tidy (${CLANG_TIDY_BIN})")
endif ()
unset(CLANG_TIDY_BIN)

#######################################################################################################################
# Apply +x permissions to all scripts in the build-support folder.
#######################################################################################################################

# CLion's remote toolchain functionality seems to lose the +x permissions when the files are rsync'd to the remote box.
# This creates a list of the executable scripts in the build-support folder and applies +x to them to ensure building
# dependent CMake targets doesn't fail.

file(GLOB_RECURSE
        BUILD_SUPPORT_SCRIPTS
        CONFIGURE_DEPENDS
        ${PROJECT_SOURCE_DIR}/build-support/*.pl
        ${PROJECT_SOURCE_DIR}/build-support/*.py
        ${PROJECT_SOURCE_DIR}/build-support/*.sh
        )

foreach (_var IN LISTS BUILD_SUPPORT_SCRIPTS)
    execute_process(COMMAND chmod +x "${_var}")
endforeach ()
