#pragma once

#include "common/object_pool.h"
#include "common/concurrent_map.h"
#include "storage/storage_defs.h"
#include "common/statistics.h"

namespace terrier {
namespace storage {
class BlockStore : public Statistics {
 public:
  explicit BlockStore(ObjectPool<RawBlock> &block_pool)
      : block_pool_(block_pool) {}
  DISALLOW_COPY_AND_MOVE(BlockStore);

  ~BlockStore() {
    for (auto it = blocks_map_.Begin(); it != blocks_map_.End(); ++it)
      block_pool_.Release(it->second);
  }

  RawBlock *RetrieveBlock(block_id_t block_id) {
    RawBlock *block = nullptr;
    blocks_map_.Find(block_id, block);
    // TODO(Matt): Is nullptr the correct return value if not present?
    return block;
  }

  std::pair<block_id_t, RawBlock *> NewBlock() {
    auto new_block_pair = std::make_pair<block_id_t, RawBlock *>(next_block_id_++, block_pool_.Get());
    blocks_map_.Insert(new_block_pair.first, new_block_pair.second);
    block_counter_ ++;
    return new_block_pair;
  }

  void UnsafeDeallocate(block_id_t block_id) {
    block_pool_.Release(RetrieveBlock(block_id));
    blocks_map_.UnsafeErase(block_id);
    block_counter_ --;
  }

  void SetStats() {
//    json_value_["block_counter"] = Json::Value(block_counter_);
  }

 private:
  ObjectPool<RawBlock> &block_pool_;
  ConcurrentMap<block_id_t, RawBlock *> blocks_map_;
  // TODO(Tianyu): the representation of block_id is subject to change
  std::atomic<block_id_t> next_block_id_;

  /** Block counter for statistics*/
  std::atomic<int> block_counter_;
};
}
}