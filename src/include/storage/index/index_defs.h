#pragma once

#include <vector>
#include "catalog/catalog_defs.h"
#include "type/type_id.h"

namespace terrier::storage::index {

/**
 * A column of the index key has an identifier, type, and describes whether it can be null.
 */
struct IndexKeyColumn {
  /**
   * Column identifier.
   */
  catalog::indexkeycol_oid_t indexkeycol_oid;
  /**
   * Column type.
   */
  type::TypeId type_id;
  /**
   * True if column is nullable.
   */
  bool is_nullable;

  /**
   * @param oid column identifier
   * @param type column data type
   * @param nullable true if the column is nullable
   */
  IndexKeyColumn(const catalog::indexkeycol_oid_t oid, const type::TypeId type, const bool nullable)
      : indexkeycol_oid(oid), type_id(type), is_nullable(nullable) {}
};

/**
 * A schema for the index key.
 * @warning the columns of the IndexKeySchema are assumed to be in comparison order
 */
using IndexKeySchema = std::vector<IndexKeyColumn>;

/**
 * The type of index.
 */
enum class ConstraintType : uint8_t {
  // invalid index constraint type
  INVALID = 0,
  // default type - not used to enforce constraints
  DEFAULT = 1,
  // used to enforce primary key constraint
  PRIMARY_KEY = 2,
  // used for unique constraint
  UNIQUE = 3
};
}  // namespace terrier::storage::index
