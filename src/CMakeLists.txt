###############################################
# Terrier sources
###############################################

file(GLOB_RECURSE TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/include/*.h)
list(REMOVE_ITEM TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/main/terrier.cpp)

# Example of adding third party sources
# file(GLOB_RECURSE EASYLOGGINGPP_SOURCES
#   ${PROJECT_SOURCE_DIR}/third_party/easylogging++/*.cc
#   ${PROJECT_SOURCE_DIR}/third_party/easylogging++/*.h
#   )
# list(APPEND TERRIER_SRCS ${EASYLOGGINGPP_SOURCES})

###############################################
# Terrier library
###############################################
add_library(terrier_shared SHARED ${TERRIER_SRCS})
target_link_libraries(terrier_shared ${TERRIER_LINK_LIBS})

###############################################
# Terrier executable
###############################################
add_executable(terrier main/terrier.cpp)
target_link_libraries(terrier terrier_shared)
