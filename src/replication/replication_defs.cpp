#include "replication/replication_defs.h"

#include "common/strong_typedef_body.h"

namespace noisepage::replication {

STRONG_TYPEDEF_BODY(msg_id_t, uint64_t);
STRONG_TYPEDEF_BODY(record_batch_id_t, uint64_t);

}  // namespace noisepage::replication
