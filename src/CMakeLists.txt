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

# spdlog
file(GLOB_RECURSE SPDLOG_SOURCES
    ${PROJECT_SOURCE_DIR}/third_party/spdlog/*.h
    )
list(APPEND TERRIER_SRCS ${SPDLOG_SOURCES})

###############################################
# Terrier library
###############################################


add_library(terrier_objlib OBJECT ${TERRIER_SRCS})
# Necessary to make static linking into other shared libraries work properly
set_property(TARGET terrier_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)
set(LIB_DEPS $<TARGET_OBJECTS:terrier_objlib>)

add_library(terrier_shared SHARED ${LIB_DEPS})
target_link_libraries(terrier_shared ${TERRIER_LINK_LIBS})

###############################################
# Terrier executable
###############################################
add_executable(terrier main/terrier.cpp)
target_link_libraries(terrier terrier_shared)
