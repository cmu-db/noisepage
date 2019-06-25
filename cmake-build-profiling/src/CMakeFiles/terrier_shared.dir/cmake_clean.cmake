file(REMOVE_RECURSE
  "../relwithdebinfo/libterrier.pdb"
  "../relwithdebinfo/libterrier.1.0.0.dylib"
  "../relwithdebinfo/libterrier.dylib"
  "../relwithdebinfo/libterrier.1.dylib"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/terrier_shared.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
