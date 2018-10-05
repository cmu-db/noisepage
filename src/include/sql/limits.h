#pragma once

#include <cfloat>

namespace terrier::sql {

static const double DBL_LOWEST = std::numeric_limits<double>::lowest();
static const double FLT_LOWEST = std::numeric_limits<float>::lowest();

static const int8_t TERRIER_INT8_MIN = (SCHAR_MIN + 1);
static const int16_t TERRIER_INT16_MIN = (SHRT_MIN + 1);
static const int32_t TERRIER_INT32_MIN = (INT_MIN + 1);
static const int64_t TERRIER_INT64_MIN = (LLONG_MIN + 1);
static const double TERRIER_DECIMAL_MIN = FLT_LOWEST;
static const uint64_t TERRIER_TIMESTAMP_MIN = 0;
static const int32_t TERRIER_DATE_MIN = (INT_MIN + 1);
static const int8_t TERRIER_BOOLEAN_MIN = 0;

static const int8_t TERRIER_INT8_MAX = SCHAR_MAX;
static const int16_t TERRIER_INT16_MAX = SHRT_MAX;
static const int32_t TERRIER_INT32_MAX = INT_MAX;
static const int64_t TERRIER_INT64_MAX = LLONG_MAX;
static const uint64_t TERRIER_UINT64_MAX = ULLONG_MAX - 1;
static const double TERRIER_DECIMAL_MAX = DBL_MAX;
static const uint64_t TERRIER_TIMESTAMP_MAX = 11231999986399999999U;  // ???
static const int32_t TERRIER_DATE_MAX = INT_MAX;
static const int8_t TERRIER_BOOLEAN_MAX = 1;

static const uint32_t TERRIER_VALUE_NULL = UINT_MAX;
static const int8_t TERRIER_INT8_NULL = SCHAR_MIN;
static const int16_t TERRIER_INT16_NULL = SHRT_MIN;
static const int32_t TERRIER_INT32_NULL = INT_MIN;
static const int64_t TERRIER_INT64_NULL = LLONG_MIN;
static const int32_t TERRIER_DATE_NULL = INT_MIN;
static const uint64_t TERRIER_TIMESTAMP_NULL = ULLONG_MAX;
static const double TERRIER_DECIMAL_NULL = DBL_LOWEST;
static const int8_t TERRIER_BOOLEAN_NULL = SCHAR_MIN;

static const uint32_t TERRIER_VARCHAR_MAX_LEN = UINT_MAX;

// Use to make TEXT type as the alias of VARCHAR(TEXT_MAX_LENGTH)
static const uint32_t TERRIER_TEXT_MAX_LEN = 1000000000;

// Objects (i.e., VARCHAR) with length prefix of -1 are NULL
#define OBJECTLENGTH_NULL -1

}  // namespace terrier::sql