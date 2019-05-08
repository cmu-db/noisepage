###############################################
# Terrier sources
###############################################

file(GLOB_RECURSE TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/include/*.h)
list(REMOVE_ITEM TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/main/terrier.cpp)
list(REMOVE_ITEM TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/execution/tpl.cpp)
list(REMOVE_ITEM TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/execution/vm/gen_opt_bc.cpp)

###############################################
# Third party sources
###############################################

# spdlog
file(GLOB_RECURSE SPDLOG_SOURCES
    ${PROJECT_SOURCE_DIR}/third_party/spdlog/*.h
    )
list(APPEND TERRIER_SRCS ${SPDLOG_SOURCES})

# libpg_query
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/libpg_query/ libpg_query.a)

# bwtree
file(GLOB_RECURSE BWTREE_SOURCES ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.cpp ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.h)
list(APPEND TERRIER_SRCS ${BWTREE_SOURCES})

# json
list(APPEND TERRIER_SRCS ${PROJECT_SOURCE_DIR}/third_party/nlohmann/json.hpp)

# ips4o
file(GLOB_RECURSE IPS4O_SOURCES
        ${PROJECT_SOURCE_DIR}/third_party/ips4o/*.hpp
        )
list(APPEND TERRIER_SRCS ${IPS4O_SOURCES})

# xbyak
file(GLOB_RECURSE XBYAK_SOURCES
        ${PROJECT_SOURCE_DIR}/third_party/xbyak/*.cpp ${PROJECT_SOURCE_DIR}/third_party/xbyak/*.hpp ${PROJECT_SOURCE_DIR}/third_party/xbyak/*.h
        )
list(APPEND TERRIER_SRCS ${XBYAK})


###############################################
# Terrier library
###############################################

ADD_TERRIER_LIB(terrier
    SOURCES ${TERRIER_SRCS}
    SHARED_LINK_FLAGS ${TERRIER_SHARED_LINK_FLAGS}
    SHARED_LINK_LIBS ${TERRIER_LINK_LIBS}
    SHARED_PRIVATE_LINK_LIBS ${TERRIERSHARED_PRIVATE_LINK_LIBS}
    STATIC_LINK_LIBS ${TERRIER_STATIC_LINK_LIBS}
    STATIC_PRIVATE_LINK_LIBS ${TERRIER_STATIC_PRIVATE_LINK_LIBS}
    )

###############################################
# Terrier executable
###############################################
set(TPL_LINK_LIBS "")
add_executable(terrier main/terrier.cpp)
target_link_libraries(terrier terrier_shared)


###############################################
# TPL executable & bytecodes
###############################################
add_subdirectory(execution)