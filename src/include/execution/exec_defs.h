#pragma once

#include "common/strong_typedef.h"

namespace terrier::execution {

STRONG_TYPEDEF_HEADER(query_id_t, uint32_t);
STRONG_TYPEDEF_HEADER(pipeline_id_t, uint32_t);
STRONG_TYPEDEF_HEADER(feature_id_t, uint32_t);
STRONG_TYPEDEF_HEADER(translator_id_t, uint32_t);

}  // namespace terrier::execution
