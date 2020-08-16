#pragma once

#include "common/object_pool.h"
#include "storage/block_allocator.h"
#include "storage/raw_block.h"

namespace terrier::storage {

/**
 * A block store is essentially an object pool. However, all blocks should be
 * aligned, so we will need to use the default constructor instead of raw
 * malloc.
 */
using BlockStore = common::ObjectPool<RawBlock, BlockAllocator>;

}  // namespace terrier::storage
