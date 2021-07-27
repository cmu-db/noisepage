#pragma once

namespace noisepage::metrics {

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
  QUERY_TRACE,
  COMPILATION
};

/**
 * Metric output types
 *
 * IF you change this enum, you must change settings_defs.h
 */
enum class MetricsOutput : uint8_t {
  NONE = 0,
  CSV,
  DB,
  CSV_AND_DB,
};

constexpr uint8_t NUM_COMPONENTS = 9;

}  // namespace noisepage::metrics
