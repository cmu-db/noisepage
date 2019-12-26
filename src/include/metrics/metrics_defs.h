#pragma once

namespace terrier::metrics {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t { LOGGING, TRANSACTION, GARBAGECOLLECTION, EXECUTION };

constexpr uint8_t NUM_COMPONENTS = 3;

}  // namespace terrier::metrics
