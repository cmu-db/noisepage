#pragma once

#include "common/strong_typedef.h"

namespace terrier::type {
STRONG_TYPEDEF(date_t, uint32_t);
STRONG_TYPEDEF(timestamp_t, uint64_t);

enum class TypeId : uint8_t {
  INVALID = 0,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INTEGER,
  BIGINT,
  DECIMAL,
  TIMESTAMP,
  DATE,
  VARCHAR,
  VARBINARY
};
}  // namespace terrier::type
