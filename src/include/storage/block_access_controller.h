#pragma once

#include <emmintrin.h>

#include <atomic>
#include <cstring>
#include <utility>

#include "common/macros.h"
#include "common/strong_typedef.h"

namespace noisepage::storage {

/**
 * Denotes the state of a block.
 */
enum class BlockState : uint32_t {
  /**
   * Hot blocks can only be accessed transactionally. Readers are forced to materialize because
   * the assumption is there are live versions.
   */
  HOT = 0,
  /**
   * This block is being marked for transformation for Arrow. Transactions are still allowed to proceed,
   * but will need to explicitly preempt the transformation process by setting this flag back to HOT.
   * Readers still need to materialize.
   */
  COOLING,
  /**
   * This block is being worked on actively by the transformation process. Transactions are not allowed to
   * write to the block, but can still read transactionally. Readers still need to materialize.
   */
  FREEZING,
  /**
   * This block is fully Arrow-compatible, and can be read in-place by readers. Transactions need to wait
   * for active readers to finish and flip block status back to hot before proceeding.
   */
  FROZEN
};

// TODO(Tianyu): I need a better name for this...
/**
 * A block access controller coordinates access among transactional workers, Arrow readers, and the background
 * transformation thread. More specifically it serves as a coarse-grained "lock" for all tuples in a block. The "lock"
 * is in quotes because not all accessor will respect the lock all the time as certain accessors have higher priorities
 * (e.g. transactional updates and reads). More specifically, transactional reads never respect the lock, and
 * transactional updates share the lock amongst themselves but have to wait for all in-place readers to finish when
 * grabbing the lock. Arrow readers will never wait on the lock as they are given low priority, and will revert to
 * reading transactionally if the block is not frozen.
 */
class BlockAccessController {
  // We do some reinterpret_casting between uint64_t and the std::pair below, so we want to assert the object size.
  static_assert(sizeof(std::pair<BlockState, uint32_t>) == sizeof(uint64_t));

 public:
  /**
   * Initialize the block access controller.
   */
  void Initialize() {
    // A new block is always hot and has no active readers
    std::memset(bytes_, 0, sizeof(uint64_t));
  }

  /**
   * Checks whether the block is safe for in-place reads. If the result returns true, the lock is successfully acquired
   * and will need to be explicitly dropped via invocation of ReleaseRead. Otherwise, the block cannot be accessed
   * in-place and the reader should try the transactional path and make a snapshot of the block.
   * @return whether reading in-place is allowed for this block.
   */
  bool TryAcquireInPlaceRead() {
    // TODO(Tianyu): This probably does not scale well. But our assumed workload is that readers will not be contending
    // for access in a tight loop, so maybe it's fine.
    while (true) {
      // We will need to compare and swap the block state and reader count together to ensure safety
      std::pair<BlockState, uint32_t> curr_state = AtomicallyLoadMembers();
      // Can only read in-place if a block is not being updated
      if (curr_state.first != BlockState::FROZEN) return false;
      // Increment reader count while holding the rest constant
      if (UpdateAtomically(curr_state, {curr_state.first, curr_state.second + 1})) return true;  // NOLINT
    }
  }

  /**
   * Releases the read lock acquired by an in-place reader on the block
   */
  void ReleaseInPlaceRead() {
    NOISEPAGE_ASSERT(GetReaderCount()->load() > 0, "Attempting to release read lock when there is none");
    // Increment reader count while holding the rest constant
    GetReaderCount()->fetch_sub(1);
  }

  /**
   * blocks until all in-place readers have left to be able to perform in-place modifications.
   */
  void WaitUntilHot() {
    while (true) {
      BlockState current_state = GetBlockState()->load();
      switch (current_state) {
        case BlockState::FREEZING:
          continue;  // Wait until the compactor finishes before doing anything
        case BlockState::COOLING:
          if (!GetBlockState()->compare_exchange_strong(current_state, BlockState::HOT)) continue;
          // wait until the compactor finishes before doing anything
          // intentional fall through
        case BlockState::FROZEN:
          GetBlockState()->store(BlockState::HOT);
          // intentional fall through
        case BlockState::HOT:
          // Although the block is already hot, we may need to wait for any straggling readers to finish
          while (GetReaderCount()->load() != 0) _mm_pause();
          break;
        default:
          throw std::runtime_error("unexpected control flow");
      }
      break;
    }
  }

  /**
   * @return state of the current block
   */
  std::atomic<BlockState> *GetBlockState() { return reinterpret_cast<std::atomic<BlockState> *>(bytes_); }

 private:
  friend class BlockCompactor;
  // we are breaking this down to two fields, (| BlockState (32-bits) | Reader Count (32-bits) |)
  // but may need to compare and swap on the two together sometimes
  byte bytes_[sizeof(uint64_t)];

  std::atomic<uint32_t> *GetReaderCount() {
    return reinterpret_cast<std::atomic<uint32_t> *>(reinterpret_cast<uint32_t *>(bytes_) + 1);
  }

  std::pair<BlockState, uint32_t> AtomicallyLoadMembers() {
    uint64_t curr_value = reinterpret_cast<std::atomic<uint64_t> *>(bytes_)->load();
    const auto *const curr_state = reinterpret_cast<const std::pair<BlockState, uint32_t> *const>(&curr_value);
    return *curr_state;
  }

  bool UpdateAtomically(const std::pair<BlockState, uint32_t> &expected, std::pair<BlockState, uint32_t> desired) {
    auto expected_bytes = *reinterpret_cast<const uint64_t *const>(&expected);
    const auto *const desired_bytes = reinterpret_cast<const uint64_t *const>(&desired);
    return reinterpret_cast<std::atomic<uint64_t> *>(bytes_)->compare_exchange_strong(expected_bytes, *desired_bytes);
  }
};
}  // namespace noisepage::storage
