#pragma once

#include <array>
#include <bitset>

namespace terrier::metrics {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { LOGGING };

constexpr uint8_t NUM_COMPONENTS = 1;

}  // namespace terrier::metrics
