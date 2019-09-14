# Util Files
file(GLOB_RECURSE UTIL_SRCS ${PROJECT_SOURCE_DIR}/util/*.cpp ${PROJECT_SOURCE_DIR}/util/include/*.h)
list(REMOVE_ITEM UTIL_SRCS ${PROJECT_SOURCE_DIR}/util/execution/tpl.cpp)
list(REMOVE_ITEM UTIL_SRCS ${PROJECT_SOURCE_DIR}/util/execution/gen_opt_bc.cpp)
list(REMOVE_ITEM UTIL_SRCS ${PROJECT_SOURCE_DIR}/util/execution/bytecode_handlers_ir.cpp)
add_library(util_static STATIC ${UTIL_SRCS})
target_link_libraries(util_static terrier_static ${TERRIER_LINK_LIBS})
add_dependencies(util_static terrier_static)


# The version number.
set(TPL_VERSION_MAJOR 0)
set(TPL_VERSION_MINOR 1)

# configure a header file to pass some of the CMake settings to the source code
configure_file(
        "execution/tpl.h.in"
        "${PROJECT_BINARY_DIR}/execution/tpl.h"
)

# Find CLANG
set(SUPPORTED_CLANGS "clang++-8" "clang++")

find_program(CLANG NAMES ${SUPPORTED_CLANGS})
if (NOT EXISTS ${CLANG})
    message(FATAL_ERROR "Unable to locate clang++.")
else ()
    message(STATUS "Found Clang ${CLANG}")
endif ()

# Setup compiler arguments to Clang
set(CLANG_IR_CXX_FLAGS "${CXX_ONLY_FLAGS} ${CXX_OPTIMIZATION_FLAGS} ${CXX_COMMON_FLAGS} -emit-llvm -c")
string(REPLACE "-fPIC" "" CLANG_IR_CXX_FLAGS ${CLANG_IR_CXX_FLAGS})

separate_arguments(CLANG_IR_CXX_FLAGS)
message(STATUS "CLANG_IR_CXX_FLAGS: ${CLANG_IR_CXX_FLAGS}")

get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach (dir ${dirs})
    list(APPEND CLANG_INCLUDES ${dir})
endforeach ()
list(REMOVE_DUPLICATES CLANG_INCLUDES)
foreach (dir ${CLANG_INCLUDES})
    set(CLANG_IR_CXX_INCLUDES ${CLANG_IR_CXX_INCLUDES} "-I${dir}")
endforeach ()
message(STATUS "CLANG_IR_CXX_INCLUDES: ${CLANG_IR_CXX_INCLUDES}")


# Build the "gen_opt_bc" executable. This will be used to clean up the ir file.
add_executable(gen_opt_bc "execution/gen_opt_bc.cpp")
set_target_properties(gen_opt_bc PROPERTIES ENABLE_EXPORTS 1)
target_link_libraries(gen_opt_bc ${TERRIER_LINK_LIBS})

##################################################
#
# TPL executable
#
##################################################
set(tpl_main "execution/tpl.cpp")
add_executable(tpl ${tpl_main})
set_target_properties(tpl PROPERTIES ENABLE_EXPORTS 1)
add_dependencies(tpl gen_opt_bc)
target_link_libraries(tpl util_static)

##################################################
#
# After TPL is built, we need to clean the bitcode module that was
# cross-compiled. Add a post-build target here ...
#
##################################################

add_custom_command(
        TARGET tpl
        POST_BUILD
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating Optimized Bitcode ..."
        COMMAND ${CLANG} ${CLANG_IR_CXX_FLAGS} ${CLANG_IR_CXX_INCLUDES}
            ${PROJECT_SOURCE_DIR}/util/execution/bytecode_handlers_ir.cpp -o bytecode_handlers_ir.bc
        COMMAND gen_opt_bc bytecode_handlers_ir.bc bytecode_handlers_opt.bc
        COMMAND mv bytecode_handlers_opt.bc bytecode_handlers_ir.bc)


##################################################
#
# Set the output folders for all binaries and libraries
#
##################################################

set_target_properties(tpl gen_opt_bc
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        )

##################################################
#
# Check TPL files
#
##################################################

add_custom_target(check-tpl
        DEPENDS tpl
        COMMAND ${BUILD_SUPPORT_DIR}/run_tpl_tests.py
        -b ${CMAKE_BINARY_DIR}/bin/tpl
        -f ${PROJECT_SOURCE_DIR}/sample_tpl/tpl_tests.txt
        -t ${PROJECT_SOURCE_DIR}/sample_tpl
        -d ${CMAKE_BINARY_DIR}
        )
