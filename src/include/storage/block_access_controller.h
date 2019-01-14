#pragma once
#include <atomic>
#include <utility>
#include "common/macros.h"
#include "common/strong_typedef.h"

namespace terrier::storage {
// TODO(Tianyu): I need a better name for this...
/**
 * A block access controller serves as a coarse-grained "lock" for all tuples in a block. The "lock" is in quotes
 * because not all accessor will respect the lock all the time as certain accessors have higher priorities (e.g.
 * transactional updates and reads). More specifically, in-place readers and transactional accessors share locks
 * amongst themselves but not with each other. In-place readers will never wait on the lock as they are given low
 * priority. Transactional accessors will have to wait for all in-place readers to finish but will prevent any other
 * in-place readers from starting.
 */
class BlockAccessController {
 private:
  enum class BlockState : uint32_t {
    HOT = 0,  // The block has recently been worked on transactionally and is likely to be worked on again in the future
    FROZEN    // The block is arrow-compatible and not being transactionally worked on
  };

 public:
  /**
   * Initialize the block access controller.
   */
  void Initialize() {
    // A new block is always hot and has no active readers
    memset(bytes_, 0, sizeof(uint64_t));
  }

  /**
   * Checks whether the block is safe for in-place reads. If the result returns true, the lock is successfully acquired
   * and will need to be explicitly dropped via invocation of ReleaseRead. Otherwise, the block cannot be accessed
   * in-place and the reader should try the transactional path and make a snapshot of the block.
   * @return whether reading in-place is allowed for this block.
   */
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

  /**
   * Releases the read lock acquired by an in-place reader on the block
   */
  void ReleaseRead() {
    while (true) {
      // We will need to compare and swap the block state and reader count together to ensure safety
      std::pair<BlockState, uint32_t> curr_state = AtomicallyLoadMembers();
      TERRIER_ASSERT(curr_state.second > 0, "Attempting to release read lock when there is none");
      // Increment reader count while holding the rest constant
      if (UpdateAtomically(curr_state.first, curr_state.second - 1, curr_state)) break;
    }
  }

  /**
   * @return whether the block is frozen (i.e. ready for in-place reads)
   */
  bool IsFrozen() { return GetBlockState()->load() == BlockState::FROZEN; }

  /**
   * blocks until all in-place readers have left to be able to perform in-place modifications.
   */
  void WaitUntilHot() {
    BlockState current_state = GetBlockState()->load();
    switch (current_state) {
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

  /**
   * Marks a block frozen and safe for in-place reads. This should only be called by the block compactor after
   * it enforces all the invariants of a frozen block.
   */
  void MarkFrozen() { GetBlockState()->store(BlockState::FROZEN); }

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
    return reinterpret_cast<std::atomic<uint64_t> *>(bytes_)->compare_exchange_strong(
        desired, *reinterpret_cast<const uint64_t *>(&expected));
  }
};
}  // namespace terrier::storage
