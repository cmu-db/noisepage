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

}  // namespace noisepage::type
