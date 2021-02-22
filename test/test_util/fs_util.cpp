#include "test_util/fs_util.h"

// Macros are not fun and should be avoided where possible, but
// this check ensures that we fail at compile time in the event
// that this definition is not present, rather than discovering
// this at runtime and throwing an exception at test initialization.
#ifndef NOISEPAGE_BUILD_ROOT
#error "NOISEPAGE_BUILD_ROOT is not defined!"
#endif

// Necessary grossness for stringizing macro expansion.
#define AS_STRING(x) #x
#define AS_EXPANDED_STRING(x) AS_STRING(x)

// Remove one call to string::append() (below)
#define SLASH_BIN_LITERAL "/bin/"

namespace noisepage::common {

std::string GetBuildRootPath() {
  // As above, relying on the path to be injected by the preprocessor.
  return AS_EXPANDED_STRING(NOISEPAGE_BUILD_ROOT);
}

std::string GetBinaryArtifactPath(std::string_view name) {
  // Note that we don't need any more macros here to do
  // concatenation of string literals because these are
  // concatenated at the language (C/C++) level
  // e.g. "str1" "str2" => "str1str2" by C standard
  // (section 6.4.5 'String Literals' of C11)
  auto path = std::string{AS_EXPANDED_STRING(NOISEPAGE_BUILD_ROOT) SLASH_BIN_LITERAL};
  path.append(name);
  return path;
}
}  // namespace noisepage::common

#undef SLASH_BIN_LITERAL
#undef AS_EXPANDED_STRING
#undef AS_STRING
