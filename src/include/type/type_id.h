#pragma once

#include "common/strong_typedef.h"

namespace noisepage::type {

/**
 * Julian date.
 * Precision: days
 * Range: 0 (Nov 24, -4713) to 2^31-1 (Jun 03, 5874898).
 */
STRONG_TYPEDEF_HEADER(date_t, uint32_t);

/**
 * Julian timestamp.
 * Precision: microseconds, 14 digits
 * Range: 4713 BC to 294276 AD
 */
STRONG_TYPEDEF_HEADER(timestamp_t, uint64_t);

// TODO(Matt): reconcile with execution::sql::SqlTypeId
// TODO(Matt): also what is noisepage::parser::ColumnDefinition::DataType?
enum class TypeId : uint8_t {
  INVALID = 0,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INTEGER,
  BIGINT,
  REAL,     // equivalent to DOUBLE
  DECIMAL,  // equivalent to NUMERIC
  TIMESTAMP,
  DATE,
  VARCHAR,
  VARBINARY,
  PARAMETER_OFFSET,
  VARIADIC,
  VAR_ARRAY,  ///< pg_type requires a distinct type for var_array.
};

}  // namespace noisepage::type
