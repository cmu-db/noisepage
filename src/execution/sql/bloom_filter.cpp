#include "execution/sql/bloom_filter.h"

#include <limits>
#include <vector>

#include "execution/util/bit_util.h"
#include "execution/util/simd.h"

namespace terrier::execution::sql {

BloomFilter::BloomFilter() = default;

BloomFilter::BloomFilter(MemoryPool *memory)
    : memory_(memory), blocks_(nullptr), block_mask_(0), lazily_added_hashes_(nullptr) {}

BloomFilter::BloomFilter(MemoryPool *memory, u32 num_elems) : BloomFilter() { Init(memory, num_elems); }

BloomFilter::~BloomFilter() {
  const auto num_bytes = GetNumBlocks() * sizeof(Block);
  memory_->Deallocate(blocks_, num_bytes);
}

void BloomFilter::Init(MemoryPool *memory, u32 num_elems) {
  memory_ = memory;
  lazily_added_hashes_ = MemPoolVector<hash_t>(memory_);

  u64 num_bits = util::MathUtil::PowerOf2Ceil(kBitsPerElement * num_elems);
  u64 num_blocks = util::MathUtil::DivRoundUp(num_bits, sizeof(Block) * kBitsPerByte);
  u64 num_bytes = num_blocks * sizeof(Block);
  blocks_ = reinterpret_cast<Block *>(memory->AllocateAligned(num_bytes, common::Constants::CACHELINE_SIZE, true));

  block_mask_ = static_cast<u32>(num_blocks - 1);
}

u64 BloomFilter::GetTotalBitsSet() const {
  u64 count = 0;
  for (u32 i = 0; i < GetNumBlocks(); i++) {
    // Note that we process 64-bits at a time, thus we only need four iterations
    // over a block. We don't use SIMD here because this function isn't
    // performance-critical.
    const auto *const chunk = reinterpret_cast<const u64 *>(blocks_[i]);
    for (u32 j = 0; j < 4; j++) {
      count += util::BitUtil::CountBits(chunk[j]);
    }
  }
  return count;
}


#if defined(__AVX2__) || defined(__AVX512F__)

// Vectorized version of Add
void BloomFilter::Add(hash_t hash) {
  auto block_idx = static_cast<u32>(hash & block_mask_);

  auto block = util::simd::Vec8().Load(blocks_[block_idx]);
  auto alt_hash = util::simd::Vec8(static_cast<u32>(hash >> 32));
  auto salts = util::simd::Vec8().Load(kSalts);

  alt_hash *= salts;
  if constexpr (util::simd::Bitwidth::value != 256) {
    // Make sure we're dealing with 32-bit values
    alt_hash &= util::simd::Vec8(std::numeric_limits<u32>::max() - 1);
  }
  alt_hash >>= 27;

  auto masks = util::simd::Vec8(1) << alt_hash;

  block |= masks;

  block.Store(blocks_[block_idx]);
}

// Vectorized version of Contains
bool BloomFilter::Contains(hash_t hash) const {
  auto block_idx = static_cast<u32>(hash & block_mask_);

  auto block = util::simd::Vec8().Load(blocks_[block_idx]);
  auto alt_hash = util::simd::Vec8(static_cast<u32>(hash >> 32));
  auto salts = util::simd::Vec8().Load(kSalts);

  alt_hash *= salts;
  if constexpr (util::simd::Bitwidth::value != 256) {
    // Make sure we're dealing with 32-bit values
    alt_hash &= util::simd::Vec8(std::numeric_limits<u32>::max() - 1);
  }
  alt_hash >>= 27;

  auto masks = util::simd::Vec8(1) << alt_hash;

  return block.AllBitsAtPositionsSet(masks);
}

#else

// Scalar version of Add
void BloomFilter::Add(hash_t hash) {
  u32 block_idx = static_cast<u32>(hash & block_mask_);
  Block &block = blocks_[block_idx];
  u32 alt_hash = static_cast<u32>(hash >> 32);
  for (u32 i = 0; i < 8; i++) {
    u32 bit_idx = (alt_hash * kSalts[i]) >> 27;
    util::BitUtil::Set(&block[i], bit_idx);
  }
}

// Scalar version of Contains
bool BloomFilter::Contains(hash_t hash) const {
  u32 alt_hash = static_cast<u32>(hash >> 32);
  u32 block_idx = static_cast<u32>(hash & block_mask_);

  Block &block = blocks_[block_idx];
  for (u32 i = 0; i < 8; i++) {
    u32 bit_idx = (alt_hash * kSalts[i]) >> 27;
    if (!util::BitUtil::Test(&block[i], bit_idx)) {
      return false;
    }
  }

  return true;
}
#endif

}  // namespace terrier::execution::sql
