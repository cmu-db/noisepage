file(REMOVE_RECURSE
  "../debug/libterrier.pdb"
  "../debug/libterrier.1.0.0.dylib"
  "../debug/libterrier.dylib"
  "../debug/libterrier.1.dylib"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/terrier_shared.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
