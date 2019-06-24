#include "common/thread_context.h"

namespace terrier::common {

thread_local common::ThreadContext thread_context{nullptr};
}
