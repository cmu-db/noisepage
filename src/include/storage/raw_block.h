#pragma once

#include "common/constants.h"
#include "storage/block_access_controller.h"
#include "storage/storage_defs.h"

namespace terrier::storage {
class DataTable;

/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * unless interpreted by a TupleAccessStrategy. The header layout is documented in the class as well.
 * @see TupleAccessStrategy
 *
 * @warning If you change the layout please also change the way header sizes are computed in block layout!
 */
class alignas(common::Constants::BLOCK_SIZE) RawBlock {
 public:
  /**
   * Data Table for this RawBlock. This is used by indexes and GC to get back to the DataTable given only a TupleSlot
   */
  DataTable *data_table_;

  /**
   * Padding for flags or whatever we may want in the future. Determined by size of layout_version below. See
   * tuple_access_strategy.h for more details on Block header layout.
   */
  uint16_t padding_;

  /**
   * Layout version.
   */
  layout_version_t layout_version_;

  /**
   * The insert head tells us where the next insertion should take place. Notice that this counter is never
   * decreased as slot recycling does not happen on the fly with insertions. A background compaction process
   * scans through blocks and free up slots.
   * Since the block size is less then (1<<20) the uppper 12 bits of insert_head_ is free. We use the first bit (1<<31)
   * to indicate if the block is insertable.
   * If the first bit is 0, the block is insertable, otherwise one txn is inserting to this block
   */
  std::atomic<uint32_t> insert_head_;
  /**
   * Access controller of this block that coordinates access among Arrow readers, transactional workers
   * and the transformation thread. In practice this can be used almost like a lock.
   */
  BlockAccessController controller_;

  /**
   * Contents of the raw block.
   */
  byte content_[common::Constants::BLOCK_SIZE - sizeof(uintptr_t) - sizeof(uint16_t) - sizeof(layout_version_t) -
                sizeof(uint32_t) - sizeof(BlockAccessController)];
  // A Block needs to always be aligned to 1 MB, so we can get free bytes to
  // store offsets within a block in one 8-byte word

  /**
   * Get the offset of this block. Because the first bit insert_head_ is used to indicate the status
   * of the block, we need to clear the status bit to get the real offset
   * @return the offset which tells us where the next insertion should take place
   */
  uint32_t GetInsertHead() { return INT32_MAX & insert_head_.load(); }
};

}  // namespace terrier::storage
