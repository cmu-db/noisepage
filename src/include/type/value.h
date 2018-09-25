#pragma once

#include "common/typedefs.h"
#include "type/type_id.h"

namespace terrier::type {
/**
 * A Value is a tagged union of a SQL type ID and the contents.
 */
struct Value {
  /**
   * SQL type ID.
   */
  TypeId type_id;
  /**
   * Contents of this value.
   */
  byte *contents;
};

}  // namespace terrier::type
