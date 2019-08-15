###############################################
# Terrier sources
###############################################

file(GLOB_RECURSE TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/include/*.h)
list(REMOVE_ITEM TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/main/terrier.cpp)

###############################################
# Third party sources
###############################################

# spdlog: used for debug logging
file(GLOB_RECURSE SPDLOG_SOURCES
        ${PROJECT_SOURCE_DIR}/third_party/spdlog/*.h
        )
list(APPEND TERRIER_SRCS ${SPDLOG_SOURCES})

# libpg_query: used for the parser
add_subdirectory(${PROJECT_SOURCE_DIR}/third_party/libpg_query/ libpg_query.a)

# bwtree: used for an index
file(GLOB_RECURSE BWTREE_SOURCES ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.cpp ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.h)
list(APPEND TERRIER_SRCS ${BWTREE_SOURCES})

# json: used for object serialization
list(APPEND TERRIER_SRCS ${PROJECT_SOURCE_DIR}/third_party/nlohmann/json.hpp)

# ips4o: used for parallel sorting in execution
file(GLOB_RECURSE IPS4O_SOURCES ${PROJECT_SOURCE_DIR}/third_party/ips4o/*.hpp)
list(APPEND TERRIER_SRCS ${IPS4O_SOURCES})

# xbyak: used for interpretation in execution
file(GLOB_RECURSE XBYAK_SOURCES ${PROJECT_SOURCE_DIR}/third_party/xbyak/*.h)
list(APPEND TERRIER_SRCS ${XBYAK_SOURCES})

# xxHash: used for hashing of large objects (strings, etc.)
file(GLOB_RECURSE XXHASH_SOURCES ${PROJECT_SOURCE_DIR}/third_party/xxHash/*.h ${PROJECT_SOURCE_DIR}/third_party/xxHash/*.c)
list(APPEND TERRIER_SRCS ${XBYAK_SOURCES})

# libcount
file(GLOB_RECURSE LIBCOUNT_SOURCES ${PROJECT_SOURCE_DIR}/third_party/libcount/*.cc ${PROJECT_SOURCE_DIR}/third_party/libcount/*.h)
list(APPEND TERRIER_SRCS ${LIBCOUNT_SOURCES})

# Sqlite 3:
find_package(Sqlite3 REQUIRED)
include_directories(SYSTEM ${SQLITE3_INCLUDE_DIRS})
list(APPEND TERRIER_LINK_LIBS ${SQLITE3_LIBRARIES})

# gflags
list(APPEND TERRIER_STATIC_LINK_LIBS gflags)

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
add_executable(terrier main/terrier.cpp)
target_link_libraries(terrier terrier_static ${TERRIER_LINK_LIBS})
