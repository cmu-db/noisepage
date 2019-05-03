#pragma once

#include <forward_list>
#include <functional>
#include <queue>
#include <utility>
#include "common/strong_typedef.h"
namespace terrier::transaction {
STRONG_TYPEDEF(timestamp_t, uint64_t);

class TransactionContext;
// Explicitly define the underlying structure of std::queue as std::list since we believe the default (std::deque) may
// be too memory inefficient and we don't need the fast random access that it provides. It's also impossible to call
// std::deque's shrink_to_fit() from the std::queue wrapper, while std::list should reduce its memory footprint
// automatically (it's a linked list). We can afford std::list's pointer-chasing because it's only processed in a
// background thread (GC). This structure can be replace with something faster if it becomes a measurable performance
// bottleneck.
using TransactionQueue = std::forward_list<transaction::TransactionContext *>;
using callback_fn = void (*)(void *);

using Action = std::function<void()>;
}  // namespace terrier::transaction
