# The version number.
set(TPL_VERSION_MAJOR 0)
set(TPL_VERSION_MINOR 1)

# configure a header file to pass some of the CMake settings to the source code
configure_file (
        "${PROJECT_SOURCE_DIR}/src/execution/tpl.h.in"
        "${PROJECT_BINARY_DIR}/execution/tpl.h"
)

# Find CLANG
set(SUPPORTED_CLANGS "clang++-7" "clang++-6.0" "clang++")

find_program(CLANG NAMES ${SUPPORTED_CLANGS})
if (NOT EXISTS ${CLANG})
    message(FATAL_ERROR "Unable to locate clang++.")
else()
    message(STATUS "Found Clang ${CLANG}")
endif ()

# Setup compiler arguments to Clang
# TODO(Amadou): Given all the CMAKE_CXX_FLAGS clang does not compile.
# For now I just use copied these flags from the TPL repo. Eventually, we should probably figure out a way to pass this in automatically.
# TODO(WAN): We really want -fno-rtti for execution, but the dummy catalog? is using TBB which needs it.
set(CLANG_CXX_FLAGS -std=c++17 -Wall -Werror -mcx16 -march=native -fdiagnostics-color=auto -ggdb -O0 -fno-omit-frame-pointer -fno-optimize-sibling-calls)
set(CLANG_IR_CXX_FLAGS ${CLANG_CXX_FLAGS} "-emit-llvm" "-c")
separate_arguments(CLANG_IR_CXX_FLAGS)
set(CLANG_IR_CXX_INCLUDES "-I${PROJECT_SOURCE_DIR}/src/include")
set(CLANG_IR_CXX_INCLUDES ${CLANG_IR_CXX_INCLUDES} "-I${PROJECT_SOURCE_DIR}/third_party/spdlog/include")
set(CLANG_IR_CXX_INCLUDES ${CLANG_IR_CXX_INCLUDES} "-I${PROJECT_SOURCE_DIR}/third_party/portable_endian")
set(CLANG_IR_CXX_INCLUDES ${CLANG_IR_CXX_INCLUDES} "-I${PROJECT_SOURCE_DIR}/third_party/bwtree")
set(CLANG_IR_CXX_INCLUDES ${CLANG_IR_CXX_INCLUDES} "-I${PROJECT_SOURCE_DIR}/third_party/")
set(CLANG_IR_CXX_INCLUDES ${CLANG_IR_CXX_INCLUDES} "-I${GFLAGS_INCLUDE_DIR}")
set(CLANG_IR_CXX_INCLUDES ${CLANG_IR_CXX_INCLUDES} "-isystem${LLVM_INCLUDE_DIRS}")

# Function to generate rule to cross compile a source file to an IR module.
# When called called with a 'src.cpp' source file it will generate a 'src_bc'
# target that can be built.
# e.g. COMPILE_TO_IR(test.cpp) generates the "test_bc" make target.
# Modified from Apache Impala.
function(tpl_compile_to_ir SRC_FILE)
    set(FULL_NAME "${PROJECT_SOURCE_DIR}/src/execution/${SRC_FILE}")
    get_filename_component(BASE_NAME ${FULL_NAME} NAME_WE)
    set(OUTPUT_FILE "${CMAKE_BINARY_DIR}/${BASE_NAME}.bc")
    add_custom_command(
            OUTPUT ${OUTPUT_FILE}
            COMMAND ${CLANG} ${CLANG_IR_CXX_FLAGS} ${CLANG_IR_CXX_INCLUDES} ${FULL_NAME} -o ${OUTPUT_FILE}
            DEPENDS ${SRC_FILE})
    add_custom_target(${BASE_NAME}_bc ALL DEPENDS ${OUTPUT_FILE})
    add_dependencies(${BASE_NAME}_bc terrier_shared)
    add_dependencies(gen_opt_bc terrier_shared)
    add_dependencies(gen_opt_bc ${BASE_NAME}_bc)
endfunction(tpl_compile_to_ir)

# Build the "gen_opt_bc" executable. This is dependent on all cross-compiled bitcode
# set up through tpl_compile_to_ir()
add_executable(gen_opt_bc "${PROJECT_SOURCE_DIR}/src/execution/vm/gen_opt_bc.cpp")
target_link_libraries(gen_opt_bc ${TERRIER_LINK_LIBS})

# Cross-compile the bytecode handlers
tpl_compile_to_ir(vm/bytecode_handlers_ir.cpp)

##################################################
#
# TPL executable
#
##################################################
set(tpl_main "${PROJECT_SOURCE_DIR}/src/execution/tpl.cpp")
add_executable(tpl ${tpl_main})
add_dependencies(tpl gen_opt_bc)
target_link_libraries(tpl terrier_shared)

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
