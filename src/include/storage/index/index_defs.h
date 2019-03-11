#pragma once

#include <vector>
#include "type/type_id.h"

namespace terrier::storage::index {

STRONG_TYPEDEF(key_oid_t, uint32_t);

struct KeyData {
  key_oid_t key_oid;
  type::TypeId type_id;
  bool is_nullable;
};

/**
 * @warning KeySchema is assumed to be in comparison order
 */
using KeySchema = std::vector<KeyData>;

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
