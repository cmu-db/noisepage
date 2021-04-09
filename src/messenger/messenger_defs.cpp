#include "messenger/messenger_defs.h"

#include "common/strong_typedef_body.h"

namespace noisepage::messenger {

STRONG_TYPEDEF_BODY(callback_id_t, uint64_t);
STRONG_TYPEDEF_BODY(connection_id_t, uint64_t);
STRONG_TYPEDEF_BODY(message_id_t, uint64_t);
STRONG_TYPEDEF_BODY(router_id_t, uint64_t);

}  // namespace noisepage::messenger
