set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/cmake/Support")

if ( "${CLANG_TIDY_BIN}" STREQUAL "CLANG_TIDY_BIN-NOTFOUND" )
    set(CLANG_TIDY_FOUND 0)
    message("-- Could NOT find clang-tidy")
else()
    set(CLANG_TIDY_FOUND 1)
    message("-- Found clang-tidy at ${CLANG_TIDY_BIN}")
endif()

############################################################
# "make clang-tidy" and "make check-clang-tidy" targets
############################################################
if (${CLANG_TIDY_FOUND})
    # runs clang-tidy and attempts to fix any warning automatically
    add_custom_target(clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json 1
            `find ${CMAKE_CURRENT_SOURCE_DIR}/src`)
    # runs clang-tidy and exits with a non-zero exit code if any errors are found.
    add_custom_target(check-clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json
            0 `find ${CMAKE_CURRENT_SOURCE_DIR}/src`)

endif()