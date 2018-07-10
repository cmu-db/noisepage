
################################################################################################
# Helper function to fetch terrier includes which will be passed to dependent projects
# Usage:
#   terrier_get_current_includes(<includes_list_variable>)
function(terrier_get_current_includes includes_variable)
  get_property(current_includes DIRECTORY PROPERTY INCLUDE_DIRECTORIES)
  terrier_convert_absolute_paths(current_includes)

  # remove at most one ${PROJECT_BINARY_DIR} include added for terrier_config.h
  list(FIND current_includes ${PROJECT_BINARY_DIR} __index)
  list(REMOVE_AT current_includes ${__index})

  # removing numpy includes (since not required for client libs)
  set(__toremove "")
  foreach(__i ${current_includes})
    if(${__i} MATCHES "python")
      list(APPEND __toremove ${__i})
    endif()
  endforeach()
  if(__toremove)
    list(REMOVE_ITEM current_includes ${__toremove})
  endif()

  terrier_list_unique(current_includes)
  set(${includes_variable} ${current_includes} PARENT_SCOPE)
endfunction()

################################################################################################
# Helper function to get all list items that begin with given prefix
# Usage:
#   terrier_get_items_with_prefix(<prefix> <list_variable> <output_variable>)
function(terrier_get_items_with_prefix prefix list_variable output_variable)
  set(__result "")
  foreach(__e ${${list_variable}})
    if(__e MATCHES "^${prefix}.*")
      list(APPEND __result ${__e})
    endif()
  endforeach()
  set(${output_variable} ${__result} PARENT_SCOPE)
endfunction()

################################################################################################
# Function for generation Terrier build- and install- tree export config files
# Usage:
#  terrier_generate_export_configs()
function(terrier_generate_export_configs)
  set(install_cmake_suffix "share/Terrier")

  # ---[ Configure build-tree TerrierConfig.cmake file ]---
  terrier_get_current_includes(Terrier_INCLUDE_DIRS)

  set(Terrier_DEFINITIONS "")

  configure_file("cmake/Templates/TerrierConfig.cmake.in" "${PROJECT_BINARY_DIR}/TerrierConfig.cmake" @ONLY)

  # Add targets to the build-tree export set
  export(TARGETS terrier FILE "${PROJECT_BINARY_DIR}/TerrierTargets.cmake")
  export(PACKAGE Terrier)

  # ---[ Configure install-tree TerrierConfig.cmake file ]---

  # remove source and build dir includes
  terrier_get_items_with_prefix(${PROJECT_SOURCE_DIR} Terrier_INCLUDE_DIRS __insource)
  terrier_get_items_with_prefix(${PROJECT_BINARY_DIR} Terrier_INCLUDE_DIRS __inbinary)
  list(REMOVE_ITEM Terrier_INCLUDE_DIRS ${__insource} ${__inbinary})

  # add `install` include folder
  set(lines
     "get_filename_component(__terrier_include \"\${Terrier_CMAKE_DIR}/../../include\" ABSOLUTE)\n"
     "list(APPEND Terrier_INCLUDE_DIRS \${__terrier_include})\n"
     "unset(__terrier_include)\n")
  string(REPLACE ";" "" Terrier_INSTALL_INCLUDE_DIR_APPEND_COMMAND ${lines})

  configure_file("cmake/Templates/TerrierConfig.cmake.in" "${PROJECT_BINARY_DIR}/cmake/TerrierConfig.cmake" @ONLY)

  # Install the TerrierConfig.cmake and export set to use with install-tree
  install(FILES "${PROJECT_BINARY_DIR}/cmake/TerrierConfig.cmake" DESTINATION ${install_cmake_suffix})
endfunction()


