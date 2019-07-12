#pragma once

#include <array>
#include <bitset>

namespace terrier::metrics {
#define METRICS_DISABLED nullptr

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { LOGGING, TRANSACTION };

constexpr uint8_t NUM_COMPONENTS = 2;

}  // namespace terrier::metrics
