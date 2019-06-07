#pragma once

#include <utility>
#include <vector>
#include "common/strong_typedef.h"

namespace terrier::storage::metric {

/**
 * Metric types
 */
enum class MetricType {
  // Metric type is invalid
  INVALID = 0,
  // Statistics for a specific database
  DATABASE = 1,
  // Statistics for a specific table
  TABLE = 2,
  // Statistics for a specific index
  INDEX = 3,
  // Statistics for a transaction
  TRANSACTION = 4,
  // For testing
  TEST = 5
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
  TABLE_MEMORY_USAGE,
  TABLE_MEMORY_RECLAIM,
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
