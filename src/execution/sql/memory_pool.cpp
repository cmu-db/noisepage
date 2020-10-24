#include "execution/sql/memory_pool.h"

#include <cstdlib>
#include <memory>

#include "common/constants.h"
#include "execution/sql/memory_tracker.h"
#include "execution/util/memory.h"

namespace noisepage::execution::sql {

// If the allocation size is larger than this value, use huge pages
std::atomic<std::size_t> MemoryPool::mmap_threshold = 64 * common::Constants::MB;

// Minimum alignment to abide by
static constexpr uint32_t MIN_MALLOC_ALIGNMENT = 8;

MemoryPool::MemoryPool(common::ManagedPointer<sql::MemoryTracker> tracker) : tracker_(tracker) { (void)tracker_; }

void *MemoryPool::AllocateAligned(const std::size_t size, const std::size_t alignment, const bool clear) {
  void *buf = nullptr;

  if (size >= mmap_threshold.load(std::memory_order_relaxed)) {
    buf = util::Memory::MallocHuge(size, true);
    NOISEPAGE_ASSERT(buf != nullptr, "Null memory pointer");
    // No need to clear memory, guaranteed on Linux
  } else {
    if (alignment < MIN_MALLOC_ALIGNMENT) {
      if (clear) {
        buf = std::calloc(size, 1);
      } else {
        buf = std::malloc(size);
      }
    } else {
      buf = util::Memory::MallocAligned(size, alignment);
      if (clear) {
        std::memset(buf, 0, size);
      }
    }
  }

  if (tracker_ != nullptr) tracker_->Increment(size);

  // Done
  return buf;
}

void MemoryPool::Deallocate(void *ptr, std::size_t size) {
  if (size >= mmap_threshold.load(std::memory_order_relaxed)) {
    util::Memory::FreeHuge(ptr, size);
  } else {
    std::free(ptr);
  }

  if (tracker_ != nullptr) tracker_->Decrement(size);
}

void MemoryPool::SetMMapSizeThreshold(const std::size_t size) { mmap_threshold = size; }

}  // namespace noisepage::execution::sql
