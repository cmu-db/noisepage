#pragma once

#include "execution/query_parameters.h"
#include "storage/varlen_pool.h"
#include "transaction/transaction_manager.h"
#include "type/value.h"

namespace terrier {

namespace transaction {
class TransactionContext;
}  // namespace transaction

namespace execution {

/**
 * @brief Stores information for one execution of a plan.
 */
class ExecutionContext {
 public:
  /// Constructor
  ExecutionContext(transaction::TransactionContext *transaction,
      transaction::TransactionManager *transaction_manager, parameters = {});

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(ExecutionContext);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Return the transaction for this particular query execution
  transaction::TransactionContext *GetTransaction() const;

  /// Return the transaction manager.
  transaction::TransactionManager *GetTransactionManager() const;

  /// Return the explicit set of parameters for this particular query execution
  const std::vector<type::Value> &GetParamValues() const;

  /// Return the query parameters
  QueryParameters &GetParams();

  /// Return the memory pool for this particular query execution
  storage::VarlenPool *GetPool();

  class ThreadStates {
   public:
    explicit ThreadStates(storage::VarlenPool &pool);

    /// Reset the state space
    void Reset(uint32_t state_size);

    /// Allocate enough state for the given number of threads
    void Allocate(uint32_t num_threads);

    /// Access the state for the thread with the given id
    char *AccessThreadState(uint32_t thread_id) const;

    /// Return the number of threads registered in this state
    uint32_t NumThreads() const { return num_threads_; }

    /// Iterate over each thread's state, operating on the element at the given
    /// offset only.
    template <typename T>
    void ForEach(uint32_t element_offset, std::function<void(T *)> func) const;

   private:
    storage::VarlenPool &pool_;
    uint32_t num_threads_;
    uint32_t state_size_;
    char *states_;
  };

  ThreadStates &GetThreadStates();

  /// Number of processed tuples during execution
  uint32_t num_processed = 0;

 private:
  // The transaction context
  transaction::TransactionContext *transaction_;
  // The transaction manager
  transaction::TransactionManager *transaction_manager_;
  // All query parameters
  QueryParameters parameters_;
  // Temporary memory pool for allocations done during execution
  storage::VarlenPool pool_;
  // Container for all states of all thread participating in this execution
  ThreadStates thread_states_;
};

template <typename T>
inline void ExecutionContext::ThreadStates::ForEach(uint32_t element_offset, std::function<void(T *)> func) const {
  TERRIER_ASSERT(element_offset < state_size_, "The element offset should be less than the state size.");
  for (uint32_t tid = 0; tid < NumThreads(); tid++) {
    auto *elem_state = reinterpret_cast<T *>(AccessThreadState(tid) + element_offset);
    func(elem_state);
  }
}

}  // namespace execution
}  // namespace terrier
