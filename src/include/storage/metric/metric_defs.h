#pragma once

#include <utility>
#include <vector>

namespace terrier::storage::metric {
/**
 * Metric types
 */
enum class MetricType {
  // Metric type is invalid
      INVALID = 0,
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
  // For testing
      TEST = 11
};

/**
 * Triggering events for stats collection
 */
enum class StatsEventType {
  TXN_BEGIN,
  TXN_COMMIT,
  TXN_ABORT,
  TUPLE_READ,
  TUPLE_UPDATE,
  TUPLE_INSERT,
  TUPLE_DELETE,
  INDEX_READ,
  INDEX_UPDATE,
  INDEX_INSERT,
  INDEX_DELETE,
  TABLE_MEMORY_ALLOC,
  TABLE_MEMORY_FREE,
  INDEX_MEMORY_ALLOC,
  INDEX_MEMORY_FREE,
  INDEX_MEMORY_USAGE,
  INDEX_MEMORY_RECLAIM,
  QUERY_BEGIN,
  QUERY_END,
  TEST  // Testing event
};

/**
 * Hasher for enums
 * @tparam E the enum class to be hashed
 */
template <class E>
class EnumHash {
 public:
  /**
   * @param e ENUM to be hashed
   * @return hashed value of the supplied enum
   */
  size_t operator()(const E &e) const {
    return std::hash<typename std::underlying_type<E>::type>()(static_cast<typename std::underlying_type<E>::type>(e));
  }
};

}  // namespace terrier::storage::metric
