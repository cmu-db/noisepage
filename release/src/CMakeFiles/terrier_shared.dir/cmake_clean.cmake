file(REMOVE_RECURSE
  "../release/libterrier.1.0.0.dylib"
  "../release/libterrier.1.dylib"
  "../release/libterrier.dylib"
  "../release/libterrier.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/terrier_shared.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
