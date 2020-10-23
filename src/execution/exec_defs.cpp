#include "execution/exec_defs.h"

#include "common/strong_typedef_body.h"

namespace noisepage::execution {

STRONG_TYPEDEF_BODY(query_id_t, uint32_t);
STRONG_TYPEDEF_BODY(pipeline_id_t, uint32_t);
STRONG_TYPEDEF_BODY(feature_id_t, uint32_t);
STRONG_TYPEDEF_BODY(translator_id_t, uint32_t);

}  // namespace noisepage::execution
