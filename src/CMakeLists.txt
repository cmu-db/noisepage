###############################################
# Terrier sources
###############################################

file(GLOB_RECURSE TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/include/*.h)
list(REMOVE_ITEM TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/main/terrier.cpp)

# spdlog
file(GLOB_RECURSE SPDLOG_SOURCES
    ${PROJECT_SOURCE_DIR}/third_party/spdlog/*.h
    )
list(APPEND TERRIER_SRCS ${SPDLOG_SOURCES})

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
add_executable(terrier main/terrier.cpp)
target_link_libraries(terrier terrier_shared)
