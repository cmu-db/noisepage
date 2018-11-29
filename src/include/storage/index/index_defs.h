#pragma once

namespace terrier::storage::index {

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
