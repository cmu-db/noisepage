#include "execution/sql/thread_state_container.h"

#include <memory>
#include <vector>

#include "common/constants.h"
#include "tbb/tbb.h"

namespace terrier::execution::sql {

// ---------------------------------------------------------
// Thread Local State Handle
// ---------------------------------------------------------

ThreadStateContainer::TLSHandle::TLSHandle() : container_(nullptr), state_(nullptr) {}

ThreadStateContainer::TLSHandle::TLSHandle(ThreadStateContainer *container) : container_(container) {
  TERRIER_ASSERT(container_ != nullptr, "Container must be non-null");
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

// ---------------------------------------------------------
// Actual container of all thread state
// ---------------------------------------------------------

/**
 * The actual container for all thread-local state for participating threads
 */
struct ThreadStateContainer::Impl {
  tbb::enumerable_thread_specific<TLSHandle> states_;
};

// ---------------------------------------------------------
// Thread State Container
// ---------------------------------------------------------

ThreadStateContainer::ThreadStateContainer(MemoryPool *memory)
    : memory_(memory),
      state_size_(0),
      init_fn_(nullptr),
      destroy_fn_(nullptr),
      ctx_(nullptr),
      impl_(std::make_unique<ThreadStateContainer::Impl>()) {
  impl_->states_ = tbb::enumerable_thread_specific<TLSHandle>([&]() { return TLSHandle(this); });
}

ThreadStateContainer::~ThreadStateContainer() = default;

void ThreadStateContainer::Clear() { impl_->states_.clear(); }

void ThreadStateContainer::Reset(const std::size_t state_size, InitFn init_fn, DestroyFn destroy_fn, void *ctx) {
  // Ensure we clean before resetting sizes_, functions, context
  Clear();

  // Now we can set these fields since all thread-local state has been cleaned
  state_size_ = state_size;
  init_fn_ = init_fn;
  destroy_fn_ = destroy_fn;
  ctx_ = ctx;
}

byte *ThreadStateContainer::AccessThreadStateOfCurrentThread() {
  auto &tls_handle = impl_->states_.local();
  return tls_handle.State();
}

void ThreadStateContainer::CollectThreadLocalStates(std::vector<byte *> *container) const {
  container->clear();
  container->reserve(impl_->states_.size());
  for (auto &tls_handle : impl_->states_) {
    container->push_back(tls_handle.State());
  }
}

void ThreadStateContainer::CollectThreadLocalStateElements(std::vector<byte *> *container,
                                                           std::size_t element_offset) const {
  container->clear();
  container->reserve(impl_->states_.size());
  for (auto &tls_handle : impl_->states_) {
    container->push_back(tls_handle.State() + element_offset);
  }
}

void ThreadStateContainer::IterateStates(void *const ctx, ThreadStateContainer::IterateFn iterate_fn) const {
  for (auto &tls_handle : impl_->states_) {
    iterate_fn(ctx, tls_handle.State());
  }
}

}  // namespace terrier::execution::sql
