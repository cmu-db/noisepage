##################################################################################
#SRC CMAKELISTS
##################################################################################

##################################################################################

# --[ Terrier library

# creates 'srcs' lists
terrier_pickup_terrier_sources(${PROJECT_SOURCE_DIR})

add_library(terrier SHARED ${srcs})

target_link_libraries(terrier PUBLIC ${TERRIER_LINKER_LIBS})

terrier_default_properties(terrier)

set(terrier_main ${PROJECT_SOURCE_DIR}/src/main/terrier.cpp)
add_executable(terrier-bin ${terrier_main})
target_link_libraries(terrier-bin terrier)
set_target_properties(terrier-bin PROPERTIES OUTPUT_NAME terrier)

##################################################################################

# --[ link to jemalloc
set(EXE_LINK_LIBRARIES ${JEMALLOC_LIBRARIES})
set(EXE_LINK_FLAGS "-Wl,--no-as-needed")
set(EXE_LIST terrier-bin)
foreach(exe_name ${EXE_LIST})
    target_link_libraries(${exe_name} ${EXE_LINK_LIBRARIES})
    if (LINUX)
        set_target_properties(${exe_name} PROPERTIES LINK_FLAGS ${EXE_LINK_FLAGS})
    endif ()
endforeach()
