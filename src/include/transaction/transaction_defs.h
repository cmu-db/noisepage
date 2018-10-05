#pragma once

#include <forward_list>
namespace terrier::transaction {
class TransactionContext;
// Explicitly define the underlying structure of std::queue as std::list since we believe the default (std::deque) may
// be too memory inefficient and we don't need the fast random access that it provides. It's also impossible to call
// std::deque's shrink_to_fit() from the std::queue wrapper, while std::list should reduce its memory footprint
// automatically (it's a linked list). We can afford std::list's pointer-chasing because it's only processed in a
// background thread (GC). This structure can be replace with something faster if it becomes a measurable performance
// bottleneck.
using TransactionQueue = std::forward_list<transaction::TransactionContext *>;

enum class ResultType {
  // TODO(Tianyu): Most of these seem unnecessary and broken. Refactor later.
  INVALID = 0,  // invalid result type
  SUCCESS,
  FAILURE,
  ABORTED ,  // aborted
  NOOP,     // no op
  UNKNOWN,
  QUEUING,
  TO_ABORT,
};
}  // namespace terrier::transaction
