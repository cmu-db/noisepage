#include "execution/sql/memory_pool.h"

#include <cstdlib>
#include <memory>

#include "execution/util/memory.h"

namespace terrier::sql {

// If the allocation size is larger than this value, use huge pages
std::atomic<std::size_t> MemoryPool::kMmapThreshold = 64 * MB;

// Minimum alignment to abide by
static constexpr u32 kMinMallocAlignment = 8;

MemoryPool::MemoryPool(MemoryTracker *tracker) : tracker_(tracker) {}

void *MemoryPool::Allocate(const std::size_t size, const bool clear) { return AllocateAligned(size, 0, clear); }

void *MemoryPool::AllocateAligned(const std::size_t size, const std::size_t alignment, const bool clear) {
  void *buf = nullptr;

  if (size >= kMmapThreshold.load(std::memory_order_relaxed)) {
    buf = util::MallocHuge(size);
    TPL_ASSERT(buf != nullptr, "Null memory pointer");
    // No need to clear memory, guaranteed on Linux
  } else {
    if (alignment < kMinMallocAlignment) {
      if (clear) {
        buf = std::calloc(size, 1);
      } else {
        buf = std::malloc(size);
      }
    } else {
      buf = std::aligned_alloc(alignment, size);
      if (clear) {
        std::memset(buf, 0, size);
      }
    }
  }

  // Done
  return buf;
}

void MemoryPool::Deallocate(void *ptr, std::size_t size) {
  if (size >= kMmapThreshold.load(std::memory_order_relaxed)) {
    util::FreeHuge(ptr, size);
  } else {
    std::free(ptr);
  }
}

void MemoryPool::SetMMapSizeThreshold(const std::size_t size) { kMmapThreshold = size; }

}  // namespace terrier::sql
