#pragma once
#include <atomic>
#include <utility>
#include "common/macros.h"
#include "common/strong_typedef.h"

namespace terrier::storage {
// TODO(Tianyu): Placeholder for the actual implementation that has the same number of bytes. We use this to make sure
//               the new offset calculations and everything works.
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
 public:
  /**
   * TODO(Tianyu): Make GCC happy
   */
  uint64_t bytes_;
};
}  // namespace terrier::storage
