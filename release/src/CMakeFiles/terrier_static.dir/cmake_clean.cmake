file(REMOVE_RECURSE
  "../release/libterrier.a"
  "../release/libterrier.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/terrier_static.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
