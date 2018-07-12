#pragma once

#include "common/object_pool.h"
#include "common/concurrent_map.h"
#include "storage/storage_defs.h"

namespace terrier {
class BlockStore {
 public:
  explicit BlockStore(ObjectPool<RawBlock> &block_pool) : block_pool_(block_pool) {}
  DISALLOW_COPY_AND_MOVE(BlockStore);
  
 private:
  ObjectPool<RawBlock> &block_pool_;
  ConcurrentMap<block_id_t, RawBlock *> blocks_map_;

};
}
