#pragma once

#include <algorithm>
#include <map>
#include <unordered_set>

#include "catalog/index_schema.h"

namespace noisepage::parser {
class AbstractExpression;
}

namespace noisepage::storage::index {

class Index;
class IndexMetadata;

/**
 * The IndexBuilder automatically creates the best possible index for the given parameters.
 */
class IndexBuilder {
 private:
  catalog::IndexSchema key_schema_;

 public:
  IndexBuilder() = default;

  /**
   * @return a new best-possible index for the current parameters, nullptr if it failed to construct a valid index
   */
  Index *Build() const;

  /**
   * @param key_schema the index key schema
   * @return the builder object
   */
  IndexBuilder &SetKeySchema(const catalog::IndexSchema &key_schema);

 private:
  template <storage::index::IndexType type, class Key>
  void ApplyIndexOptions(Index *index) const;

  Index *BuildBwTreeIntsKey(IndexMetadata metadata) const;

  Index *BuildBwTreeGenericKey(IndexMetadata metadata) const;

  Index *BuildBPlusTreeIntsKey(IndexMetadata &&metadata) const;

  Index *BuildBPlusTreeGenericKey(IndexMetadata metadata) const;

  Index *BuildHashIntsKey(IndexMetadata metadata) const;

  Index *BuildHashGenericKey(IndexMetadata metadata) const;
};

}  // namespace noisepage::storage::index
