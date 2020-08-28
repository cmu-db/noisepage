#include "execution/sql/thread_state_container.h"

#include <tbb/mutex.h>
#include <tbb/parallel_for_each.h>

#include <memory>
#include <thread>  //NOLINT
#include <unordered_map>

#include "common/constants.h"

namespace terrier::execution::sql {

//===----------------------------------------------------------------------===//
//
// Thread Local State Handle
//
//===----------------------------------------------------------------------===//

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

//===----------------------------------------------------------------------===//
//
// Actual container of all thread state
//
//===----------------------------------------------------------------------===//

// The actual container for all thread-local state for participating threads
struct ThreadStateContainer::Impl {
  tbb::mutex states_mutex_;
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
      impl_(std::make_unique<ThreadStateContainer::Impl>()) {}

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
  tbb::mutex::scoped_lock lock(impl_->states_mutex_);
  if (impl_->states_.find(std::this_thread::get_id()) == impl_->states_.end()) {
    std::unique_ptr<TLSHandle> tls_handle(new TLSHandle(this));
    impl_->states_.insert(std::make_pair(std::this_thread::get_id(), std::move(tls_handle)));
  }
  return impl_->states_.find(std::this_thread::get_id())->second->State();
}

void ThreadStateContainer::CollectThreadLocalStates(std::vector<byte *> *container) const {
  container->clear();
  tbb::mutex::scoped_lock lock(impl_->states_mutex_);
  container->reserve(impl_->states_.size());
  for (auto &tls_handle : impl_->states_) {
    container->push_back(tls_handle.second->State());
  }
}

void ThreadStateContainer::CollectThreadLocalStateElements(std::vector<byte *> *container,
                                                           const std::size_t element_offset) const {
  container->clear();
  tbb::mutex::scoped_lock lock(impl_->states_mutex_);
  container->reserve(impl_->states_.size());
  for (auto &tls_handle : impl_->states_) {
    container->push_back(tls_handle.second->State() + element_offset);
  }
}

void ThreadStateContainer::IterateStates(void *const ctx, ThreadStateContainer::IterateFn iterate_fn) const {
  tbb::mutex::scoped_lock lock(impl_->states_mutex_);
  for (auto &tls_handle : impl_->states_) {
    iterate_fn(ctx, tls_handle.second->State());
  }
}

void ThreadStateContainer::IterateStatesParallel(void *const ctx, ThreadStateContainer::IterateFn iterate_fn) const {
  tbb::mutex::scoped_lock lock(impl_->states_mutex_);
  tbb::parallel_for_each(impl_->states_, [&](auto &tls_handle) { iterate_fn(ctx, tls_handle.second->State()); });
}

uint32_t ThreadStateContainer::GetThreadStateCount() const { return impl_->states_.size(); }

}  // namespace terrier::execution::sql
