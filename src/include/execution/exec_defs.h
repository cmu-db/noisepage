#pragma once

#include "common/strong_typedef.h"

namespace terrier::execution {

constexpr uint32_t NULL_ID = 0;

STRONG_TYPEDEF_HEADER(query_id_t, uint32_t);
STRONG_TYPEDEF_HEADER(pipeline_id_t, uint32_t);
STRONG_TYPEDEF_HEADER(feature_id_t, uint32_t);
STRONG_TYPEDEF_HEADER(translator_id_t, uint32_t);

constexpr pipeline_id_t INVALID_PIPELINE_ID = pipeline_id_t(NULL_ID);

}  // namespace terrier::execution
