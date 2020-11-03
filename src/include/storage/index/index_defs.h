#pragma once

#include <array>

#include "type/type_id.h"

namespace noisepage::storage::index {
/**
 * This enum indicates the backing implementation that should be used for the index.  It is a character enum in order
 * to better match PostgreSQL's look and feel when persisted through the catalog.
 */
enum class IndexType : char { BWTREE = 'B', HASHMAP = 'H' };

/**
 * Internal enum to stash with the index to represent its key type. We don't need to persist this.
 */
enum class IndexKeyKind : uint8_t { COMPACTINTSKEY, GENERICKEY, HASHKEY };

/**
 * Types that can be used in simple keys, i.e. CompactIntsKey and HashKey
 */
constexpr std::array<type::TypeId, 4> NUMERIC_KEY_TYPES{type::TypeId::TINYINT, type::TypeId::SMALLINT,
                                                        type::TypeId::INTEGER, type::TypeId::BIGINT};

enum class ScanType : uint8_t {
  Closed,   /* [low, high] range scan */
  OpenLow,  /* [begin(), high] range scan */
  OpenHigh, /* [low, end()] range scan */
  OpenBoth  /* [begin(), end()] range scan */
};

}  // namespace noisepage::storage::index
