#pragma once

namespace terrier::storage::index {
/**
 * This enum indicates the backing implementation that should be used for the index.  It is a character enum in order
 * to better match PostgreSQL's look and feel when persisted through the catalog.
 */
enum class IndexType : char { BWTREE = 'B', HASHMAP = 'H' };
}  // namespace terrier::storage::index
