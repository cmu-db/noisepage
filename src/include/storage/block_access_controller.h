#pragma once
#include <atomic>
#include "common/macros.h"

namespace terrier::storage {
// TODO(Tianyu): I need a better name for this...
class BlockAccessController {
 private:
  enum class BlockState : uint32_t {
    HOT = 0,  // The block has recently been worked on transactionally and is likely to be worked on again in the future
    HEATING, // The block has active in-place readers but a transactional writer wants it
    FREEZING, // The block has not been worked on in a while and is being compacted
    FROZEN // The block is arrow-compatible and not being transactionally worked on
  };
 public:

  void Initialize() {
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
      uint64_t  new_bytes = curr_bytes;
      uint32_t &reader_count = reinterpret_cast<uint32_t *>(&new_bytes)[1];
      reader_count++;
      if (bytes_.compare_exchange_strong(curr_bytes, new_bytes)) return true;
    }
  }

  void WaitUntilHot() {
    BlockState currrent_state = GetBlockState()->load();
    switch (currrent_state) {
      case BlockState::FROZEN:
        // We will have to flip the state to HOT to prevent readers from working on the block in place,
        // but first we need to wait for all the readers to leave.
        GetBlockState()->store(BlockState::HEATING);
        // intentional fall through
      case BlockState::HEATING:
        // TODO(Tianyu): Is there a better way to spin?
        // need to wait for readers to finish
        while (GetReaderCount()->load() != 0) __asm__ __volatile__("pause;");
        // intentional fall through
      case BlockState::HOT:
        // Although the block is already hot, we need to do a blind store to make sure that if we coincide
        // with a compacting transaction, our existence is made known to them.
        GetBlockState()->store(BlockState::HOT);
        return;
      case BlockState::FREEZING:
        // we can still immediately start working on this block without waiting, but need to flip state back to hot
        while (!GetBlockState()->compare_exchange_strong(currrent_state, BlockState::HOT))
          // It is possible that some other thread has already changed the state of block (e.g. finished compaction and
          // flipped to FROZEN, and readers have started working on the block in place), it is essential that we are
          // aware of such changes because otherwise we risk not synchronizing properly
          WaitUntilHot();
        break;
      default:
        throw std::runtime_error("unexpected control flow");
    }
  }

  void MarkFreezing() {
    GetBlockState()->store(BlockState::FREEZING);
  }

  void MarkFrozen() {
    GetBlockState()->store(BlockState::FROZEN);
  }

 private:
  // we are breaking this down to two fields, (| BlockState (32-bits) | Reader Count (32-bits) |)
  // but may need to compare and swap on the two together sometimes
  std::atomic<uint64_t> bytes_;

  std::atomic<BlockState> *GetBlockState() {
    return reinterpret_cast<std::atomic<BlockState> *>(&bytes_);
  }

  std::atomic<uint32_t> *GetReaderCount() {
    return reinterpret_cast<std::atomic<uint32_t> *>(reinterpret_cast<uint32_t *>(&bytes_) + 1);
  }
};
}  // namespace terrier::storage
