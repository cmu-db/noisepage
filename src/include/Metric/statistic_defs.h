#pragma once

#include <utility>
#include <vector>

namespace terrier::stats {
/**
 * Metric types
 */
enum class MetricType {
  // Metric type is invalid
  INVALID = INVALID_TYPE_ID,
  // Metric to count a number
  COUNTER = 1,
  // Access information, e.g., # tuples read, inserted, updated, deleted
  ACCESS = 2,
  // Life time of a object
  LIFETIME = 3,
  // Statistics for a specific database
  DATABASE = 4,
  // Statistics for a specific table
  TABLE = 5,
  // Statistics for a specific index
  INDEX = 6,
  // Latency of transactions
  LATENCY = 7,
  // Timestamp, e.g., creation time of a table/index
  TEMPORAL = 8,
  // Statistics for a specific table
  QUERY = 9,
  // Statistics for CPU
  PROCESSOR = 10,
};

}  // namespace terrier::stats
