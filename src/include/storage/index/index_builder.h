#pragma once

#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "storage/index/bwtree_index.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/generic_key.h"
#include "storage/index/index.h"
#include "storage/index/index_defs.h"
#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"

namespace terrier::storage::index {

/**
 * The IndexBuilder automatically creates the best possible index for the given parameters.
 */
class IndexBuilder {
 private:
  catalog::index_oid_t index_oid_{0};
  ConstraintType constraint_type_ = ConstraintType::INVALID;
  IndexKeySchema key_schema_;

 public:
  IndexBuilder() = default;

  /**
   * @return a new best-possible index for the current parameters
   */
  Index *Build() const;

  /**
   * @param index_oid the index oid
   * @return the builder object
   */
  IndexBuilder &SetOid(const catalog::index_oid_t &index_oid);

  /**
   * @param constraint_type the type of index
   * @return the builder object
   */
  IndexBuilder &SetConstraintType(const ConstraintType &constraint_type);

  /**
   * @param key_schema the index key schema
   * @return the builder object
   */
  IndexBuilder &SetKeySchema(const IndexKeySchema &key_schema);

 private:
  /**
   * @return true if attr_type can be represented with CompactIntsKey
   */
  static bool CompactIntsOk(type::TypeId attr_type) {
    switch (attr_type) {
      case type::TypeId::TINYINT:
      case type::TypeId::SMALLINT:
      case type::TypeId::INTEGER:
      case type::TypeId::BIGINT:
        return true;
      default:
        break;
    }
    return false;
  }

  /**
   * Build an empty BwTree with integer key.
   *
   * @param index_oid the oid of the index
   * @param constraint_type the type of constraint
   * @param key_size the size of the key
   * @param metadata the metadata about the index
   * @return the pointer to the empty index
   */
  Index *BuildBwTreeIntsKey(catalog::index_oid_t index_oid, ConstraintType constraint_type, uint32_t key_size,
                            IndexMetadata metadata) const;

  /**
   * Build an empty BwTree with generic key.
   *
   * @param index_oid the oid of the index
   * @param constraint_type the type of constraint
   * @param metadata the metadata about the index
   * @return the pointer to the empty index
   */
  Index *BuildBwTreeGenericKey(catalog::index_oid_t index_oid, ConstraintType constraint_type,
                               IndexMetadata metadata) const;
};

}  // namespace terrier::storage::index
