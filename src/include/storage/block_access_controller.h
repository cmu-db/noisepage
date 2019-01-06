#pragma once
#include <atomic>
#include "common/macros.h"
#include "common/strong_typedef.h"

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
    memset(bytes_, 0, sizeof(uint64_t));
  }

  bool TryAcquireRead() {
    // TODO(Tianyu): This probably does not scale well. But our assumed workload is that readers will not be contending
    // for access in a tight loop, so maybe it's fine.
    while (true) {
      // We will need to compare and swap the block state and reader count together to ensure safety
      std::pair<BlockState, uint32_t> curr_state = AtomicallyLoadMembers();
      // Can only read in-place if a block is not being updated
      if (curr_state.first != BlockState::FROZEN) return false;
      // Increment reader count while holding the rest constant
      if (UpdateAtomically(curr_state.first, curr_state.second + 1, curr_state)) return true;
    }
  }

  bool IsFrozen() { return GetBlockState()->load() == BlockState::FROZEN; }

  void WaitUntilHot() {
    BlockState current_state = GetBlockState()->load();
    switch (current_state) {
      case BlockState::FROZEN:GetBlockState()->store(BlockState::HOT);
        // intentional fall through
      case BlockState::HOT:
        // Although the block is already hot, we may need to wait for any straggling readers to finish
        while (GetReaderCount()->load() != 0) __asm__ __volatile__("pause;");
        break;
      default:throw std::runtime_error("unexpected control flow");
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
  byte bytes_[sizeof(uint64_t)];

  std::atomic<BlockState> *GetBlockState() { return reinterpret_cast<std::atomic<BlockState> *>(bytes_); }

  std::atomic<uint32_t> *GetReaderCount() {
    return reinterpret_cast<std::atomic<uint32_t> *>(reinterpret_cast<uint32_t *>(bytes_) + 1);
  }

  std::pair<BlockState, uint32_t> AtomicallyLoadMembers() {
    uint64_t curr_value = reinterpret_cast<std::atomic<uint64_t> *>(bytes_)->load();
    // mask off respective bytes to turn the two into 32 bit values
    return {static_cast<BlockState>(curr_value >> 32), curr_value & UINT32_MAX};
  }

  bool UpdateAtomically(BlockState new_state, uint32_t reader_count, const std::pair<BlockState, uint32_t> &expected) {
    uint64_t desired = (static_cast<uint64_t>(new_state) << 32) + reader_count;
    return reinterpret_cast<std::atomic<uint64_t> *>(bytes_)
        ->compare_exchange_strong(desired, *reinterpret_cast<const uint64_t *>(&expected));
  }
};
}  // namespace terrier::storage
