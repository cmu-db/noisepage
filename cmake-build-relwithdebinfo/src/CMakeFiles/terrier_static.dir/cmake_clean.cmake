file(REMOVE_RECURSE
  "../relwithdebinfo/libterrier.a"
  "../relwithdebinfo/libterrier.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/terrier_static.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
