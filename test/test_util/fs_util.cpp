#include "test_util/fs_util.h"

#include <filesystem>
#include <stdexcept>

// Macros are not fun and should be avoided where possible, but
// this check ensures that we fail at compile time in the event
// that this definition is not present, rather than discovering
// this at runtime and throwing an exception at test initialization.
#ifndef NOISEPAGE_PROJECT_ROOT
#error "NOISEPAGE_PROJECT_ROOT is not defined!"
#endif

// Necessary grossness for stringizing macro expansion.
#define AS_STRING(x) #x
#define AS_EXPANDED_STRING(x) AS_STRING(x)

namespace noisepage::common {
std::string GetProjectRootPath() {
  // Here, we rely on the project root path being "injected" into the
  // translation unit during compilation because this allows us to
  // define this function agnostic of the particular name of the root
  // directory as it exists on any one particular system.
  return AS_EXPANDED_STRING(NOISEPAGE_PROJECT_ROOT);
}

std::string FindFileFrom(std::string_view filename, std::string_view root_path) {
  namespace fs = std::filesystem;
  const auto goal = fs::path{filename};
  const auto root = fs::path{root_path};
  const auto options = fs::directory_options::skip_permission_denied;
  for (auto &entry : fs::recursive_directory_iterator{root_path, options}) {
    auto &path = entry.path();
    if (goal == path.filename()) {
      return path.string();
    }
  }
  // TODO(Kyle): Should we be using a project-specific exception here?
  // None of the existing ones in the project appear to fit this use case
  throw std::runtime_error{"Requested file not found"};
}
}  // namespace noisepage::common

#undef AS_STRING
#undef AS_EXPANDED_STRING
