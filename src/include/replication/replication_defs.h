#pragma once

#include "common/strong_typedef.h"

namespace noisepage::replication {

constexpr uint64_t NULL_ID = 0;  ///< 0 is reserved as an error/uninitialized value.

STRONG_TYPEDEF_HEADER(msg_id_t, uint64_t);
STRONG_TYPEDEF_HEADER(record_batch_id_t, uint64_t);

constexpr msg_id_t INVALID_MSG_ID{NULL_ID};                    ///< Invalid message.
constexpr record_batch_id_t INVALID_RECORD_BATCH_ID{NULL_ID};  ///< Invalid batch.

}  // namespace noisepage::replication
