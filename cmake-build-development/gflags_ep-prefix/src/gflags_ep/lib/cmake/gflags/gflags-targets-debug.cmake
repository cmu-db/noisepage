#----------------------------------------------------------------
# Generated CMake target import file for configuration "DEBUG".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "gflags_static" for configuration "DEBUG"
set_property(TARGET gflags_static APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(gflags_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libgflags.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS gflags_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_gflags_static "${_IMPORT_PREFIX}/lib/libgflags.a" )

# Import target "gflags_nothreads_static" for configuration "DEBUG"
set_property(TARGET gflags_nothreads_static APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(gflags_nothreads_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_DEBUG "CXX"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libgflags_nothreads.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS gflags_nothreads_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_gflags_nothreads_static "${_IMPORT_PREFIX}/lib/libgflags_nothreads.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
