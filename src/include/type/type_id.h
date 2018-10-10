#pragma once

#include "common/typedefs.h"

namespace terrier::type {
/**
 * All of our possible SQL types
 */
enum class TypeId : uint8_t {
  INVALID = 0,
  PARAMETER_OFFSET,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INTEGER,
  BIGINT,
  DECIMAL,
  TIMESTAMP,
  DATE,
  VARCHAR,
  VARBINARY,
  ARRAY,
  UDT
};

}  // namespace terrier::type
