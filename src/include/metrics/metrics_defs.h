#pragma once

namespace terrier::metrics {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { LOGGING, TRANSACTION };

constexpr uint8_t NUM_COMPONENTS = 2;

}  // namespace terrier::metrics
