#pragma once

namespace terrier::metric {

/**
 * Metric types
 */
enum class MetricsScope : uint8_t { SYSTEM, DATABASE, TABLE, INDEX, TRANSACTION, QUERY };

/**
 * Triggering events for stats collection
 */
enum class MetricsEventType {
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
  QUERY_END
};

}  // namespace terrier::metric
