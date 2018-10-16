#include "execution/execution_context.h"

// TODO(Justin):
// -figure out ephemeral pool replacement
// -replace GetInstance with const ref arg?
// -CACHELINE_SIZE

namespace terrier {
namespace execution {

////////////////////////////////////////////////////////////////////////////////
///
/// ExecutionContext
///
////////////////////////////////////////////////////////////////////////////////

ExecutionContext::ExecutionContext(transaction::TransactionContext *transaction, QueryParameters parameters)
    : transaction_(transaction), parameters_(std::move(parameters)), thread_states_(pool_) {}

transaction::TransactionContext *ExecutionContext::GetTransaction() const { return transaction_; }

const std::vector<type::Value> &ExecutionContext::GetParamValues() const { return parameters_.GetParameterValues(); }

QueryParameters &ExecutionContext::GetParams() { return parameters_; }

type::EphemeralPool *ExecutionContext::GetPool() { return &pool_; }

ExecutionContext::ThreadStates &ExecutionContext::GetThreadStates() { return thread_states_; }

////////////////////////////////////////////////////////////////////////////////
///
/// ThreadStates
///
////////////////////////////////////////////////////////////////////////////////

ExecutionContext::ThreadStates::ThreadStates(type::EphemeralPool &pool)
    : pool_(pool), num_threads_(0), state_size_(0), states_(nullptr) {}

void ExecutionContext::ThreadStates::Reset(const uint32_t state_size) {
  if (states_ != nullptr) {
    pool_.Free(states_);
    states_ = nullptr;
  }
  num_threads_ = 0;
  // Always fill out to nearest cache-line to prevent false sharing of states
  // between different threads.
  uint32_t pad = state_size & ~CACHELINE_SIZE;
  state_size_ = state_size + (pad != 0 ? CACHELINE_SIZE - pad : pad);
}

void ExecutionContext::ThreadStates::Allocate(const uint32_t num_threads) {
  TERRIER_ASSERT(state_size_ > 0, "State size must be positive.");
  TERRIER_ASSERT(states_ == nullptr, "States field should be null before allocation.");
  num_threads_ = num_threads;
  uint32_t alloc_size = num_threads_ * state_size_;
  states_ = reinterpret_cast<char *>(pool_.Allocate(alloc_size));
  TERRIER_MEMSET(states_, 0, alloc_size);
}

char *ExecutionContext::ThreadStates::AccessThreadState(const uint32_t thread_id) const {
  TERRIER_ASSERT(state_size_ > 0, "State size must be positive.");
  TERRIER_ASSERT(states_ != nullptr, "States field should not be null when accessing active thread state.");
  TERRIER_ASSERT(thread_id < num_threads_, "Thread id must be in [0, num_threads - 1].");
  return states_ + (thread_id * state_size_);
}

}  // namespace execution
}  // namespace terrier
