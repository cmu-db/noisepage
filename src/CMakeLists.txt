###############################################
# Terrier sources
###############################################

file(GLOB_RECURSE TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/include/*.h)
list(REMOVE_ITEM TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/main/terrier.cpp)

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
file(GLOB_RECURSE BWTREE_SOURCES
        ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.cpp
        ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.h
)
list(APPEND TERRIER_SRCS ${BWTREE_SOURCES})

# json
list(APPEND TERRIER_SRCS ${PROJECT_SOURCE_DIR}/third_party/nlohmann/json.hpp)

# madoka
file(GLOB_RECURSE MADOKA_SOURCES
        ${PROJECT_SOURCE_DIR}/third_party/madoka/*.cc
        ${PROJECT_SOURCE_DIR}/third_party/madoka/*.h
)
list(APPEND TERRIER_SRCS ${MADOKA_SOURCES})

# libcount
file(GLOB_RECURSE LIBCOUNT_SOURCES
        ${PROJECT_SOURCE_DIR}/third_party/libcount/*.cc
        ${PROJECT_SOURCE_DIR}/third_party/libcount/*.h
)
list(APPEND TERRIER_SRCS ${LIBCOUNT_SOURCES})

# ips4o: used for parallel sorting in execution
file(GLOB_RECURSE IPS4O_SOURCES ${PROJECT_SOURCE_DIR}/third_party/ips4o/*.hpp)
list(APPEND TERRIER_SRCS ${IPS4O_SOURCES})

# xbyak: used for interpretation in execution
file(GLOB_RECURSE XBYAK_SOURCES ${PROJECT_SOURCE_DIR}/third_party/xbyak/*.h)
list(APPEND TERRIER_SRCS ${XBYAK_SOURCES})

# xxHash: used for hashing of large objects (strings, etc.)
file(GLOB_RECURSE XXHASH_SOURCES ${PROJECT_SOURCE_DIR}/third_party/xxHash/*.h)
list(APPEND TERRIER_SRCS ${XXHASH_SOURCES})

# libcuckoo
file(GLOB_RECURSE LIBCUCKOO_SOURCES ${PROJECT_SOURCE_DIR}/third_party/libcuckoo/*.hh)
list(APPEND TERRIER_SRCS ${LIBCUCKOO_SOURCES})

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
        DEPENDENCIES ${TERRIER_DEPENDENCIES}
)

###############################################
# Terrier executable
###############################################
add_executable(terrier main/terrier.cpp storage/replication/replication_manager.cpp)
set_target_properties(terrier PROPERTIES ENABLE_EXPORTS 1)
target_link_libraries(terrier terrier_static ${TERRIER_LINK_LIBS})
