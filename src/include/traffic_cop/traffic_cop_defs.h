#pragma once
#include <string_view>
#include <variant>

namespace terrier::trafficcop {

/**
 * Prefix of per connection temporary namespaces
 */
static constexpr std::string_view TEMP_NAMESPACE_PREFIX = "pg_temp_";


enum class ResultType : uint8_t { COMPLETE, ERROR, NOTICE, NOOP, QUEUING, UNKNOWN };

struct TrafficCopResult {
  ResultType type_;
  std::variant<uint32_t, std::string> extra_;
};

}  // namespace terrier::trafficcop
