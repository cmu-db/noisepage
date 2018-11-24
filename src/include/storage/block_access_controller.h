#pragma once
#include <atomic>
#include "common/macros.h"

namespace terrier::storage {
// TODO(Tianyu): I need a better name for this...
class BlockAccessController {
 private:
  enum class BlockState : uint32_t {
    HOT = 0,  // The block has recently been worked on transactionally and is likely to be worked on again in the future
    FROZEN    // The block is arrow-compatible and not being transactionally worked on
  };

 public:
  void Initialize() {
    // A new block is always hot and has no active readers
    bytes_ = 0;
  }

  bool TryAcquireRead() {
    // TODO(Tianyu): This probably does not scale well. But our assumed workload is that readers will not be contending
    // for access in a tight loop, so maybe it's fine.
    while (true) {
      // We will need to compare and swap the block state and reader count together to ensure safety
      uint64_t curr_bytes = bytes_.load();
      BlockState state = *reinterpret_cast<BlockState *>(&curr_bytes);
      // Can only read in-place if a block ids
      if (state != BlockState::FROZEN) return false;
      // Increment reader count while holding the rest constant
      uint64_t new_bytes = curr_bytes;
      uint32_t &reader_count = reinterpret_cast<uint32_t *>(&new_bytes)[1];
      reader_count++;
      if (bytes_.compare_exchange_strong(curr_bytes, new_bytes)) return true;
    }
  }

  bool IsFrozen() { return GetBlockState()->load() == BlockState::FROZEN; }

  void WaitUntilHot() {
    BlockState currrent_state = GetBlockState()->load();
    switch (currrent_state) {
      case BlockState::FROZEN:
        GetBlockState()->store(BlockState::HOT);
        // intentional fall through
      case BlockState::HOT:
        // Although the block is already hot, we may need to wait for any straggling readers to finish
        while (GetReaderCount()->load() != 0) __asm__ __volatile__("pause;");
        break;
      default:
        throw std::runtime_error("unexpected control flow");
    }
  }

  void MarkFrozen() {
    // Should only mark hot blocks frozen in case another running transaction's update is lost
    BlockState state = BlockState::HOT;
    GetBlockState()->compare_exchange_strong(state, BlockState::FROZEN);
    // Who cares if it fails, best effort anyways.
  }

 private:
  // we are breaking this down to two fields, (| BlockState (32-bits) | Reader Count (32-bits) |)
  // but may need to compare and swap on the two together sometimes
  std::atomic<uint64_t> bytes_;

  std::atomic<BlockState> *GetBlockState() { return reinterpret_cast<std::atomic<BlockState> *>(&bytes_); }

  std::atomic<uint32_t> *GetReaderCount() {
    return reinterpret_cast<std::atomic<uint32_t> *>(reinterpret_cast<uint32_t *>(&bytes_) + 1);
  }
};
}  // namespace terrier::storage
