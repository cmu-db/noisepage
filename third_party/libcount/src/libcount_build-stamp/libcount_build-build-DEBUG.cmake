

set(command "make")
execute_process(
  COMMAND ${command}
  RESULT_VARIABLE result
  OUTPUT_FILE "/home/amlatyr/Code/terrier/third_party/libcount/src/libcount_build-stamp/libcount_build-build-out.log"
  ERROR_FILE "/home/amlatyr/Code/terrier/third_party/libcount/src/libcount_build-stamp/libcount_build-build-err.log"
  )
if(result)
  set(msg "Command failed: ${result}\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  set(msg "${msg}\nSee also\n  /home/amlatyr/Code/terrier/third_party/libcount/src/libcount_build-stamp/libcount_build-build-*.log")
  message(FATAL_ERROR "${msg}")
else()
  set(msg "libcount_build build command succeeded.  See also /home/amlatyr/Code/terrier/third_party/libcount/src/libcount_build-stamp/libcount_build-build-*.log")
  message(STATUS "${msg}")
endif()
