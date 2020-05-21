#pragma once

namespace terrier::metrics {

/**
 * Metric types
 */
enum class MetricsComponent : uint8_t {
  LOGGING,
  TRANSACTION,
  GARBAGECOLLECTION,
  EXECUTION,
  EXECUTION_PIPELINE,
  BIND_COMMAND,
  EXECUTE_COMMAND,
};

constexpr uint8_t NUM_COMPONENTS = 7;

}  // namespace terrier::metrics
