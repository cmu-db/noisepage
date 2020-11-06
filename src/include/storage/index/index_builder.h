#pragma once

#include "catalog/index_schema.h"

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
  Index *BuildBwTreeIntsKey(IndexMetadata metadata) const;

  Index *BuildBwTreeGenericKey(IndexMetadata metadata) const;

  Index *BuildHashIntsKey(IndexMetadata metadata) const;

  Index *BuildHashGenericKey(IndexMetadata metadata) const;
};

}  // namespace noisepage::storage::index
