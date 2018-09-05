#pragma once

#include "nlohmann/json.hpp"

namespace terrier::common {
/**
 * Convenience alias for a JSON object from the nlohmann::json library.
 */
using json = nlohmann::json;
}  // namespace terrier::common
