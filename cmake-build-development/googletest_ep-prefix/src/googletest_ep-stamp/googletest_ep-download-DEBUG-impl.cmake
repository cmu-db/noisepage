set(command "/Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake;-P;/Users/vivianhuang/Desktop/terrier/cmake-build-development/googletest_ep-prefix/src/googletest_ep-stamp/download-googletest_ep.cmake")

execute_process(COMMAND ${command} RESULT_VARIABLE result)
if(result)
  set(msg "Command failed (${result}):\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  message(FATAL_ERROR "${msg}")
endif()
set(command "/Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake;-P;/Users/vivianhuang/Desktop/terrier/cmake-build-development/googletest_ep-prefix/src/googletest_ep-stamp/verify-googletest_ep.cmake")

execute_process(COMMAND ${command} RESULT_VARIABLE result)
if(result)
  set(msg "Command failed (${result}):\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  message(FATAL_ERROR "${msg}")
endif()
set(command "/Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake;-P;/Users/vivianhuang/Desktop/terrier/cmake-build-development/googletest_ep-prefix/src/googletest_ep-stamp/extract-googletest_ep.cmake")

execute_process(COMMAND ${command} RESULT_VARIABLE result)
if(result)
  set(msg "Command failed (${result}):\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  message(FATAL_ERROR "${msg}")
endif()
