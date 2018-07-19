#pragma once

#include "common/object_pool.h"
#include "common/concurrent_map.h"
#include "storage/storage_defs.h"
#include "common/statistics.h"

namespace terrier {
namespace storage {
/**
 * Provides a mapping from block_id to the physical memory location of the
 * block.
 *
 * This class should be the only source of blocks in the system.
 */
class BlockStore : public Statistics {
 public:
  /**
   * Initializes the block store with the given object pool
   * @param block_pool object pool to use
   */
  explicit BlockStore(ObjectPool<RawBlock> &block_pool)
      : block_pool_(block_pool) {}
  DISALLOW_COPY_AND_MOVE(BlockStore);


  ~BlockStore() override {
    for (auto it = blocks_map_.Begin(); it != blocks_map_.End(); ++it)
      block_pool_.Release(it->second);
  }

  /**
   * Retrieves the memory location of the block with the given id
   * @param block_id id of the block to retrieve
   * @return ptr to the start of the block, or nullptr if id is invalid.
   */
  RawBlock *RetrieveBlock(block_id_t block_id) {
    RawBlock *block = nullptr;
    blocks_map_.Find(block_id, block);
    // TODO(Matt): Is nullptr the correct return value if not present?
    return block;
  }

  /**
   * Allocates a new block.
   * @return a pair of the id of the new block and ptr to its head.
   */
  std::pair<block_id_t, RawBlock *> NewBlock() {
    auto new_block_pair = std::make_pair<block_id_t, RawBlock *>(next_block_id_++, block_pool_.Get());
    blocks_map_.Insert(new_block_pair.first, new_block_pair.second);
    block_counter_ ++;
    return new_block_pair;
  }

  /**
   * Deallocates a block. This method is not safe to call concurrently.
   * @param block_id the id of the block to deallocate.
   */
  void UnsafeDeallocate(block_id_t block_id) {
    block_pool_.Release(RetrieveBlock(block_id));
    blocks_map_.UnsafeErase(block_id);
    block_counter_ --;
  }

  void SetStats() override {
//    json_value_["block_counter"] = Json::Value(block_counter_);
  }

 private:
  ObjectPool<RawBlock> &block_pool_;
  ConcurrentMap<block_id_t, RawBlock *> blocks_map_;
  // TODO(Tianyu): the representation of block_id is subject to change
  std::atomic<block_id_t> next_block_id_;

  /** Block counter for statistics*/
  std::atomic<int> block_counter_ {};
};
}
}