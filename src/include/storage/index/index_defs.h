#pragma once

#include <array>

namespace noisepage::storage::index {
/**
 * This enum indicates the backing implementation that should be used for the index.  It is a character enum in order
 * to better match PostgreSQL's look and feel when persisted through the catalog.
 */
enum class IndexType : char { BWTREE = 'B', HASHMAP = 'H', BPLUSTREE = 'P' };

/**
 * Internal enum to stash with the index to represent its key type. We don't need to persist this.
 */
enum class IndexKeyKind : uint8_t { COMPACTINTSKEY, GENERICKEY, HASHKEY };

/**
 * Types that can be used in simple keys, i.e. CompactIntsKey and HashKey
 */
constexpr std::array<execution::sql::SqlTypeId, 4> NUMERIC_KEY_TYPES{
    execution::sql::SqlTypeId::TinyInt, execution::sql::SqlTypeId::SmallInt, execution::sql::SqlTypeId::Integer,
    execution::sql::SqlTypeId::BigInt};

enum class ScanType : uint8_t {
  Closed,   /* [low, high] range scan */
  OpenLow,  /* [begin(), high] range scan */
  OpenHigh, /* [low, end()] range scan */
  OpenBoth  /* [begin(), end()] range scan */
};

}  // namespace noisepage::storage::index
