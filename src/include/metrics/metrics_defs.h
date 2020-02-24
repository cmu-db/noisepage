#pragma once

namespace terrier::metrics {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { LOGGING, TRANSACTION, GARBAGECOLLECTION, EXECUTION, EXECUTION_PIPELINE };

constexpr uint8_t NUM_COMPONENTS = 5;

}  // namespace terrier::metrics
