#pragma once

#include "common/strong_typedef.h"

namespace noisepage::replication {

STRONG_TYPEDEF_HEADER(msg_id_t, uint64_t);
STRONG_TYPEDEF_HEADER(record_batch_id_t, uint64_t);

}  // namespace noisepage::replication
