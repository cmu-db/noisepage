#pragma once

#include <vector>
#include "catalog/catalog_defs.h"
#include "type/type_id.h"

namespace terrier::storage::index {

struct IndexKeyColumn {
  catalog::indexkeycol_oid_t indexkeycol_oid;
  type::TypeId type_id;
  bool is_nullable;

  IndexKeyColumn(const catalog::indexkeycol_oid_t oid, const type::TypeId type, const bool nullable)
      : indexkeycol_oid(oid), type_id(type), is_nullable(nullable) {}
};

/**
 * @warning KeySchema is assumed to be in comparison order
 */
using IndexKeySchema = std::vector<IndexKeyColumn>;

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
