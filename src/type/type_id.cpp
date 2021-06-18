#include "type/type_id.h"

#include "common/strong_typedef_body.h"

namespace noisepage::type {

/**
 * Julian date.
 * Precision: days
 * Range: 0 (Nov 24, -4713) to 2^31-1 (Jun 03, 5874898).
 */
STRONG_TYPEDEF_BODY(date_t, uint32_t);

/**
 * Julian timestamp.
 * Precision: microseconds, 14 digits
 * Range: 4713 BC to 294276 AD
 */
STRONG_TYPEDEF_BODY(timestamp_t, uint64_t);

std::ostream &operator<<(std::ostream &os, TypeId type_id) {
  switch (type_id) {
    case TypeId::INVALID:
      os << "INVALID";
      break;
    case TypeId::BOOLEAN:
      os << "BOOLEAN";
      break;
    case TypeId::TINYINT:
      os << "TINYINT";
      break;
    case TypeId::SMALLINT:
      os << "SMALLINT";
      break;
    case TypeId::INTEGER:
      os << "INTEGER";
      break;
    case TypeId::BIGINT:
      os << "BIGINT";
      break;
    case TypeId::REAL:
      os << "REAL";
      break;
    case TypeId::DECIMAL:
      os << "DECIMAL";
      break;
    case TypeId::TIMESTAMP:
      os << "TIMESTAMP";
      break;
    case TypeId::DATE:
      os << "DATE";
      break;
    case TypeId::VARCHAR:
      os << "VARCHAR";
      break;
    case TypeId::VARBINARY:
      os << "VARBINARY";
      break;
    case TypeId::PARAMETER_OFFSET:
      os << "PARAMETER_OFFSET";
      break;
    case TypeId::VARIADIC:
      os << "VARIADIC";
      break;
    case TypeId::VAR_ARRAY:
      os << "VAR_ARRAY";
      break;
    default:
      NOISEPAGE_ASSERT(false, "Invalid Type ID");
  }
  return os;
}

}  // namespace noisepage::type
