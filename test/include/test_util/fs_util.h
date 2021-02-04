#pragma once

#include <string>
#include <string_view>

// TODO(Kyle): should this functionality be wrapped in another nested namespace?
// Maybe its just me that feels putting fs stuff out into the top-level namespace
// when std wraps it up into its own...
namespace noisepage {
/**
 * Determine the absolute path to the project root directory (e.g. noisepage/).
 * @return The absolute path to the project root, as a string.
 */
std::string GetProjectRootPath();

/**
 * Determine the absolute path to the file with the given name by performing a
 * recursive directory search from the specified root path.
 * @param filename The name of the target file
 * @param root_path The absolute path from which recursive search should begin
 * @return The absolute path to a file with the specified name as a non-owning string reference.
 * @throw std::runtime_error in the event the file cannot be found.
 */
std::string FindFileFrom(std::string_view filename, std::string_view root_path);

}  // namespace noisepage
