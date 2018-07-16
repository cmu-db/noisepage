################################################################################################
# Convenient command to setup source group for IDEs that support this feature (VS, XCode)
# Usage:
#   terrier_source_group(<group> GLOB[_RECURSE] <globbing_expression>)
function(terrier_source_group group)
  cmake_parse_arguments(TERRIER_SOURCE_GROUP "" "" "GLOB;GLOB_RECURSE" ${ARGN})
  if(TERRIER_SOURCE_GROUP_GLOB)
    file(GLOB srcs1 ${TERRIER_SOURCE_GROUP_GLOB})
    source_group(${group} FILES ${srcs1})
  endif()

  if(TERRIER_SOURCE_GROUP_GLOB_RECURSE)
    file(GLOB_RECURSE srcs2 ${TERRIER_SOURCE_GROUP_GLOB_RECURSE})
    source_group(${group} FILES ${srcs2})
  endif()
endfunction()

################################################################################################
# Collecting sources from globbing and appending to output list variable
# Usage:
#   terrier_collect_sources(<output_variable> GLOB[_RECURSE] <globbing_expression>)
function(terrier_collect_sources variable)
  cmake_parse_arguments(TERRIER_COLLECT_SOURCES "" "" "GLOB;GLOB_RECURSE" ${ARGN})
  if(TERRIER_COLLECT_SOURCES_GLOB)
    file(GLOB srcs1 ${TERRIER_COLLECT_SOURCES_GLOB})
    set(${variable} ${variable} ${srcs1})
  endif()

  if(TERRIER_COLLECT_SOURCES_GLOB_RECURSE)
    file(GLOB_RECURSE srcs2 ${TERRIER_COLLECT_SOURCES_GLOB_RECURSE})
    set(${variable} ${variable} ${srcs2})
  endif()
endfunction()

################################################################################################
# Short command getting terrier sources (assuming standard Terrier code tree)
# Usage:
#   terrier_pickup_terrier_sources(<root>)
function(terrier_pickup_terrier_sources root)
  # put all files in source groups (visible as subfolder in many IDEs)
  terrier_source_group("Include"        GLOB "${root}/src/include/*/*.h")
  terrier_source_group("Include"        GLOB "${PROJECT_BINARY_DIR}/terrier_config.h*")
  terrier_source_group("Source"         GLOB "${root}/src/*/*.cpp")

  # collect files
  file(GLOB_RECURSE hdrs ${root}/include/*/*.h*)
  file(GLOB_RECURSE srcs ${root}/src/*/*.cpp)
  file(GLOB_RECURSE main_srcs ${root}/src/main/*.cpp)

  # convert to absolute paths
  terrier_convert_absolute_paths(srcs)
  terrier_convert_absolute_paths(main_srcs)

  # remove test files and main files from file set
  list(REMOVE_ITEM  srcs ${main_srcs})

  # ART
  file(GLOB_RECURSE art_srcs ${root}/third_party/adaptive_radix_tree/*.cpp)
  
  # Easylogging++
  file(GLOB_RECURSE elpp_srcs ${root}/third_party/easylogging++/*.cc)

  # adding headers to make the visible in some IDEs (Qt, VS, Xcode)
  list(APPEND srcs ${hdrs} ${PROJECT_BINARY_DIR}/terrier_config.h)
  list(APPEND test_srcs ${test_hdrs})

  # add proto to make them editable in IDEs too
  file(GLOB_RECURSE proto_files ${root}/src/terrier/*.proto)
  list(APPEND srcs ${proto_files} ${murmur_srcs} ${libcount_srcs} ${art_srcs} ${elpp_srcs} ${jsoncpp_srcs})

  # propogate to parent scope
  set(srcs ${srcs} PARENT_SCOPE)
endfunction()

################################################################################################
# Short command for setting defeault target properties
# Usage:
#   terrier_default_properties(<target>)
function(terrier_default_properties target)
  set_target_properties(${target} PROPERTIES
    LINKER_LANGUAGE CXX
    ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/lib"
    LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/lib"
    RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")
  # make sure we build all external depepdencies first
  if (DEFINED external_project_dependencies)
    add_dependencies(${target} ${external_project_dependencies})
  endif()
endfunction()

################################################################################################
# Short command for setting runtime directory for build target
# Usage:
#   terrier_set_runtime_directory(<target> <dir>)
function(terrier_set_runtime_directory target dir)
  set_target_properties(${target} PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${dir}")
endfunction()

################################################################################################
# Short command for setting solution folder property for target
# Usage:
#   terrier_set_solution_folder(<target> <folder>)
function(terrier_set_solution_folder target folder)
  if(USE_PROJECT_FOLDERS)
    set_target_properties(${target} PROPERTIES FOLDER "${folder}")
  endif()
endfunction()

################################################################################################
# Reads lines from input file, prepends source directory to each line and writes to output file
# Usage:
#   terrier_configure_testdatafile(<testdatafile>)
function(terrier_configure_testdatafile file)
  file(STRINGS ${file} __lines)
  set(result "")
  foreach(line ${__lines})
    set(result "${result}${PROJECT_SOURCE_DIR}/${line}\n")
  endforeach()
  file(WRITE ${file}.gen.cmake ${result})
endfunction()

################################################################################################
# Filter out all files that are not included in selected list
# Usage:
#   terrier_leave_only_selected_tests(<filelist_variable> <selected_list>)
function(terrier_leave_only_selected_tests file_list)
  if(NOT ARGN)
    return() # blank list means leave all
  endif()
  string(REPLACE "," ";" __selected ${ARGN})
  list(APPEND __selected terrier_main)

  set(result "")
  foreach(f ${${file_list}})
    get_filename_component(name ${f} NAME_WE)
    string(REGEX REPLACE "^test_" "" name ${name})
    list(FIND __selected ${name} __index)
    if(NOT __index EQUAL -1)
      list(APPEND result ${f})
    endif()
  endforeach()
  set(${file_list} ${result} PARENT_SCOPE)
endfunction()
