#pragma once

#include <string>
#include <string_view>

namespace noisepage::common {
/**
 * Determine the absolute path to the build directory for the current build (e.g. noisepage/build/)
 * @return The absolute path to the build directory, as a string.
 */
std::string GetBuildRootPath();

/**
 * Determine the absolute path to the build artifact identified by `name`.
 *
 * By using this function, you are expressing the assumption that the artifact
 * identified by `name` is located in the binary directory for the current build.
 * In other words, `name` is located at: `GetBuildRootPath()/bin/<name>`.
 *
 * @param name The name of the artifact in question
 * @return The absolute path to the binary artifact, as a string.
 */
std::string GetBinaryArtifactPath(std::string_view name);
}  // namespace noisepage::common
