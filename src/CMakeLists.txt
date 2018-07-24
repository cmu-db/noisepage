###############################################
# Terrier sources
###############################################

file(GLOB_RECURSE TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/*.cpp ${PROJECT_SOURCE_DIR}/src/include/*.h)
list(REMOVE_ITEM TERRIER_SRCS ${PROJECT_SOURCE_DIR}/src/main/terrier.cpp)

# easylogging++
file(GLOB_RECURSE EASYLOGGINGPP_SOURCES
    ${PROJECT_SOURCE_DIR}/third_party/easylogging++/*.cc
    ${PROJECT_SOURCE_DIR}/third_party/easylogging++/*.h
    )
list(APPEND TERRIER_SRCS ${EASYLOGGINGPP_SOURCES})

###############################################
# Terrier library
###############################################
add_library(terrier_shared SHARED ${TERRIER_SRCS})
target_link_libraries(terrier_shared ${TERRIER_LINK_LIBS})

###############################################
# Terrier executable
###############################################
add_executable(terrier main/terrier.cpp)
target_link_libraries(terrier terrier_shared ${JEMALLOC_LIBRARIES})
#set_target_properties(terrier-bin PROPERTIES OUTPUT_NAME terrier)

# link jemalloc to executable
if (LINUX)
  set(JEMALLOC_LINK_FLAGS "-Wl,--no-as-needed")
  set_target_properties(${exe_name} PROPERTIES LINK_FLAGS ${JEMALLOC_LINK_FLAGS})
endif ()
