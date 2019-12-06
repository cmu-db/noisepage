#pragma once
#include <string_view>

namespace terrier::trafficcop {

/**
 * Prefix of per connection temporary namespaces
 */
static constexpr std::string_view TEMP_NAMESPACE_PREFIX = "pg_temp_";

}  // namespace terrier::trafficcop
