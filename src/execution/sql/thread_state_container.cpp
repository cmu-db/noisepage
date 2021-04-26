#include "execution/sql/thread_state_container.h"

#include <tbb/parallel_for_each.h>

#include <memory>
#include <thread>  //NOLINT
#include <unordered_map>

#include "common/constants.h"
#include "common/spin_latch.h"

namespace noisepage::execution::sql {

//===----------------------------------------------------------------------===//
//
// Thread Local State Handle
//
//===----------------------------------------------------------------------===//

ThreadStateContainer::TLSHandle::TLSHandle() : container_(nullptr), state_(nullptr) {}

ThreadStateContainer::TLSHandle::TLSHandle(ThreadStateContainer *container) : container_(container) {
  NOISEPAGE_ASSERT(container_ != nullptr, "Container must be non-null");
  const auto state_size = container_->state_size_;
  state_ =
      static_cast<byte *>(container_->memory_->AllocateAligned(state_size, common::Constants::CACHELINE_SIZE, true));

  if (auto init_fn = container_->init_fn_; init_fn != nullptr) {
    init_fn(container_->ctx_, state_);
  }
}

ThreadStateContainer::TLSHandle::~TLSHandle() {
  if (auto destroy_fn = container_->destroy_fn_; destroy_fn != nullptr) {
    destroy_fn(container_->ctx_, state_);
  }

  const auto state_size = container_->state_size_;
  container_->memory_->Deallocate(state_, state_size);
}

//===----------------------------------------------------------------------===//
//
// Actual container of all thread state
//
//===----------------------------------------------------------------------===//

// The actual container for all thread-local state for participating threads
struct ThreadStateContainer::Impl {
  common::SpinLatch states_latch_;
  std::unordered_map<std::thread::id, std::unique_ptr<TLSHandle>> states_;
};

//===----------------------------------------------------------------------===//
//
// Thread State Container
//
//===----------------------------------------------------------------------===//

ThreadStateContainer::ThreadStateContainer(MemoryPool *memory)
    : memory_(memory),
      state_size_(0),
      init_fn_(nullptr),
      destroy_fn_(nullptr),
      ctx_(nullptr),
      impl_(std::make_unique<ThreadStateContainer::Impl>()) {
  impl_->states_.reserve(2 * std::thread::hardware_concurrency());
}

ThreadStateContainer::~ThreadStateContainer() { Clear(); }

void ThreadStateContainer::Clear() { impl_->states_.clear(); }

void ThreadStateContainer::Reset(const std::size_t state_size, const ThreadStateContainer::InitFn init_fn,
                                 const ThreadStateContainer::DestroyFn destroy_fn, void *const ctx) {
  // Ensure we clean before resetting sizes, functions, context
  Clear();

  // Now we can set these fields since all thread-local state has been cleaned
  state_size_ = state_size;
  init_fn_ = init_fn;
  destroy_fn_ = destroy_fn;
  ctx_ = ctx;
}

byte *ThreadStateContainer::AccessCurrentThreadState() {
  common::SpinLatch::ScopedSpinLatch guard(&impl_->states_latch_);
  auto &tls_handle = impl_->states_[std::this_thread::get_id()];
  if (!tls_handle) {
    tls_handle = std::make_unique<TLSHandle>(this);
  }
  return tls_handle->State();
}

void ThreadStateContainer::CollectThreadLocalStates(std::vector<byte *> *container) const {
  container->clear();
  common::SpinLatch::ScopedSpinLatch guard(&impl_->states_latch_);
  container->reserve(impl_->states_.size());
  for (auto &[tid, tls_handle] : impl_->states_) {
    container->push_back(tls_handle->State());
  }
}

void ThreadStateContainer::CollectThreadLocalStateElements(std::vector<byte *> *container,
                                                           const std::size_t element_offset) const {
  container->clear();
  common::SpinLatch::ScopedSpinLatch guard(&impl_->states_latch_);
  container->reserve(impl_->states_.size());
  for (auto &[tid, tls_handle] : impl_->states_) {
    container->push_back(tls_handle->State() + element_offset);
  }
}

void ThreadStateContainer::IterateStates(void *const ctx, ThreadStateContainer::IterateFn iterate_fn) const {
  common::SpinLatch::ScopedSpinLatch guard(&impl_->states_latch_);
  for (auto &[tid, tls_handle] : impl_->states_) {
    iterate_fn(ctx, tls_handle->State());
  }
}

void ThreadStateContainer::IterateStatesParallel(void *const ctx, ThreadStateContainer::IterateFn iterate_fn) const {
  tbb::parallel_for_each(impl_->states_.begin(), impl_->states_.end(),
                         [&](auto &entry) { iterate_fn(ctx, entry.second->State()); });
}

uint32_t ThreadStateContainer::GetThreadStateCount() const { return impl_->states_.size(); }

}  // namespace noisepage::execution::sql
