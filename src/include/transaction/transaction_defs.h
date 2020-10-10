#pragma once

#include <forward_list>
#include <functional>
#include <queue>
#include <utility>

#include "common/strong_typedef.h"

namespace terrier::transaction {
STRONG_TYPEDEF_HEADER(timestamp_t, uint64_t);

constexpr uint8_t MIN_GC_INVOCATIONS = 3;

// Invalid txn timestamp. Used for validation.
static constexpr timestamp_t INVALID_TXN_TIMESTAMP = timestamp_t(INT64_MIN);

// First txn timestamp that can be given out by the txn manager
static constexpr timestamp_t INITIAL_TXN_TIMESTAMP = timestamp_t(0);

class TransactionContext;
class DeferredActionManager;

// Explicitly define the underlying structure of std::queue as std::list since we believe the default (std::deque) may
// be too memory inefficient and we don't need the fast random access that it provides. It's also impossible to call
// std::deque's shrink_to_fit() from the std::queue wrapper, while std::list should reduce its memory footprint
// automatically (it's a linked list). We can afford std::list's pointer-chasing because it's only processed in a
// background thread (GC). This structure can be replace with something faster if it becomes a measurable performance
// bottleneck.
using TransactionQueue = std::forward_list<transaction::TransactionContext *>;
using callback_fn = void (*)(void *);

/**
 * A TransactionEndAction is applied when the transaction is either committed or aborted (as configured).
 * It is given a handle to the DeferredActionManager in case it needs to register a deferred action.
 */
using TransactionEndAction = std::add_pointer<void(DeferredActionManager *)>::type;
using TransactionEndAction2 = std::add_pointer<void()>::type;

class TransactionEndActionBaseFunctor {
 public:
  virtual void operator()(DeferredActionManager *deferred_action_manager) = 0;
};

/**
 * Functor to capture the derred action function
 */
class TransactionEndActionFunc : public TransactionEndActionBaseFunctor {
 public:
  /**
   * Function with the end action
   */
  TransactionEndAction end_func_;

  /**
   * Constructor takes as arguments a function which will be stored in this functor
   * @param end_func function to execute upon end action
   */
  explicit TransactionEndActionFunc(TransactionEndAction end_func) : end_func_(end_func) {};

  /**
   * Callback the carries out the deferred action
   */
  void operator()(DeferredActionManager *deferred_action_manager) { end_func_(deferred_action_manager); }
};

/**
 * Functor to capture the derred action function
 */
class TransactionEndActionFunc2 : public TransactionEndActionBaseFunctor {
 public:
  /**
   * Function with the end action
   */
  TransactionEndAction2 end_func_;

  /**
   * Constructor takes as arguments a function which will be stored in this functor
   * @param end_func function to execute upon end action
   */
  explicit TransactionEndActionFunc2(TransactionEndAction2 end_func) : end_func_(end_func) {};

  /**
   * Callback the carries out the deferred action
   */
  void operator()(DeferredActionManager *deferred_action_manager) { end_func_(); }
};

/**
 * A DeferredAction is an action that can only be safely performed after all transactions that could
 * have access to something has finished. (e.g. pruning of version chains)
 *
 * When applied, the start time of the oldest transaction alive in the system is supplied. The reason
 * for this is that this value can be larger than the timestamp the action originally registered for,
 * and in cases such as GC knowing the actual time enables optimizations.
 */
using DeferredAction = std::function<void(timestamp_t)>;
}  // namespace terrier::transaction
