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
file(GLOB_RECURSE BWTREE_SOURCES ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.cpp ${PROJECT_SOURCE_DIR}/third_party/bwtree/*.h)
list(APPEND TERRIER_SRCS ${BWTREE_SOURCES})

# json
list(APPEND TERRIER_SRCS ${PROJECT_SOURCE_DIR}/third_party/nlohmann/json.hpp)

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
add_executable(terrier main/terrier.cpp loggers/network_logger.cpp include/loggers/network_logger.h)
target_link_libraries(terrier terrier_shared)
