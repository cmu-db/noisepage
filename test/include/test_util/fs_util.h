#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

/**
 * The name of the project root.
 *
 * TODO(Kyle): should this be read from some global configuration elsewhere?
 */
constexpr static const char *const PROJECT_ROOT = "noisepage";

// TODO(Kyle): should this functionality be wrapped in another nested namespace?
// Maybe its just me that feels putting fs stuff out into the top-level namespace
// when std wraps it up into its own...
namespace noisepage {
namespace detail {
/**
 * Determine the absolute system path to the file or directory with the specified name.
 * This function assumes the desired file or directory is located "above" the source
 * directory in the hierarchy of the filesystem in question.
 * @param name The name of the parent file or directory
 * @param src (optional) The location from which the search for descendant path should begin
 * @return The absolute path to the descandant file or directory; empty optional indicates failure.
 */
std::optional<std::filesystem::path> GetAbsolutePathToDescendant(
    std::string_view name, const std::filesystem::path &src = std::filesystem::current_path()) {
  namespace fs = std::filesystem;
  const auto dst = fs::path{name};
  auto current = src;
  while (current.filename() != dst && current.has_parent_path()) {
    const auto tmp = current.parent_path();
    if (tmp == current) {
      // The parent of the current path is itself; this occurs (on Linux) for "/",
      // which indicates failure to locate the path of interest
      return std::nullopt;
    }
    current = tmp;
  }
  if (!current.has_parent_path()) {
    return std::nullopt;
  }
  return current;
}
}  // namespace detail

/**
 * Determine the absolute path to the project root directory (noisepage/).
 * @return The absolute path to the project root, as a string.
 * @throw std::runtime_error in the event the project root cannot be located.
 */
std::string GetProjectRootPath() {
  auto path = detail::GetAbsolutePathToDescendant(PROJECT_ROOT);
  if (!path.has_value()) {
    // TODO(Kyle): should we be using a project-specific exception here?
    // None of the existing ones in the project appear to fit this use case
    throw std::runtime_error{"Failed to locate project root directory"};
  }
  return path.value().string();
}

/**
 * Determine the absolute path to the file with the given name by performing a
 * recursive directory search from the specified root path.
 * @param filename The name of the target file
 * @param root_path The absolute path from which recursive search should begin
 * @return The absolute path to a file with the specified name as a string.
 * @throw std::runtime_error in the event the file cannot be found.
 */
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
  // TODO(Kyle): should we be using a project-specific exception here?
  // None of the existing ones in the project appear to fit this use case
  throw std::runtime_error{"Requested file not found"};
}
}  // namespace noisepage
