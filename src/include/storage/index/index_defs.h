#pragma once

namespace terrier::storage::index {
/**
 * The type of index.
 */
enum class ConstraintType : uint8_t {
  // invalid index constraint type
  INVALID = 0,
  // default type - not used to enforce constraints
  DEFAULT = 1,
  // used for unique constraint
  UNIQUE = 2
};
}  // namespace terrier::storage::index
