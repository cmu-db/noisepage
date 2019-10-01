#include "execution/sql/bloom_filter.h"

#include <limits>
#include <vector>

#include "execution/util/bit_util.h"
#include "execution/util/simd.h"

namespace terrier::execution::sql {

BloomFilter::BloomFilter() = default;

BloomFilter::BloomFilter(MemoryPool *memory)
    : memory_(memory), blocks_(nullptr), block_mask_(0), lazily_added_hashes_(nullptr) {}

BloomFilter::BloomFilter(MemoryPool *memory, uint32_t num_elems) : BloomFilter() { Init(memory, num_elems); }

BloomFilter::~BloomFilter() {
  const auto num_bytes = GetNumBlocks() * sizeof(Block);
  memory_->Deallocate(blocks_, num_bytes);
}

void BloomFilter::Init(MemoryPool *memory, uint32_t num_elems) {
  memory_ = memory;
  lazily_added_hashes_ = MemPoolVector<hash_t>(memory_);

  uint64_t num_bits = common::MathUtil::PowerOf2Ceil(K_BITS_PER_ELEMENT * num_elems);
  uint64_t num_blocks = common::MathUtil::DivRoundUp(num_bits, sizeof(Block) * common::Constants::K_BITS_PER_BYTE);
  uint64_t num_bytes = num_blocks * sizeof(Block);
  blocks_ = reinterpret_cast<Block *>(memory->AllocateAligned(num_bytes, common::Constants::CACHELINE_SIZE, true));

  block_mask_ = static_cast<uint32_t>(num_blocks - 1);
}

uint64_t BloomFilter::GetTotalBitsSet() const {
  uint64_t count = 0;
  for (uint32_t i = 0; i < GetNumBlocks(); i++) {
    // Note that we process 64-bits at a time, thus we only need four iterations
    // over a block. We don't use SIMD here because this function isn't
    // performance-critical.
    const auto *const chunk = reinterpret_cast<const uint64_t *>(blocks_[i]);
    for (uint32_t j = 0; j < 4; j++) {
      count += util::BitUtil::CountBits(chunk[j]);
    }
  }
  return count;
}

#if defined(__AVX2__) || defined(__AVX512F__)

// Vectorized version of Add
void BloomFilter::Add(hash_t hash) {
  auto block_idx = static_cast<uint32_t>(hash & block_mask_);

  auto block = util::simd::Vec8().Load(blocks_[block_idx]);
  auto alt_hash = util::simd::Vec8(static_cast<uint32_t>(hash >> 32));
  auto salts = util::simd::Vec8().Load(K_SALTS);

  alt_hash *= salts;
  if constexpr (util::simd::Bitwidth::VALUE != 256) {
    // Make sure we're dealing with 32-bit values
    alt_hash &= util::simd::Vec8(std::numeric_limits<uint32_t>::max() - 1);
  }
  alt_hash >>= 27;

  auto masks = util::simd::Vec8(1) << alt_hash;

  block |= masks;

  block.Store(blocks_[block_idx]);
}

// Vectorized version of Contains
bool BloomFilter::Contains(hash_t hash) const {
  auto block_idx = static_cast<uint32_t>(hash & block_mask_);

  auto block = util::simd::Vec8().Load(blocks_[block_idx]);
  auto alt_hash = util::simd::Vec8(static_cast<uint32_t>(hash >> 32));
  auto salts = util::simd::Vec8().Load(K_SALTS);

  alt_hash *= salts;
  if constexpr (util::simd::Bitwidth::VALUE != 256) {
    // Make sure we're dealing with 32-bit values
    alt_hash &= util::simd::Vec8(std::numeric_limits<uint32_t>::max() - 1);
  }
  alt_hash >>= 27;

  auto masks = util::simd::Vec8(1) << alt_hash;

  return block.AllBitsAtPositionsSet(masks);
}

#else

// Scalar version of Add
void BloomFilter::Add(hash_t hash) {
  uint32_t block_idx = static_cast<uint32_t>(hash & block_mask_);
  Block &block = blocks_[block_idx];
  uint32_t alt_hash = static_cast<uint32_t>(hash >> 32);
  for (uint32_t i = 0; i < 8; i++) {
    uint32_t bit_idx = (alt_hash * K_SALTS[i]) >> 27;
    util::BitUtil::Set(&block[i], bit_idx);
  }
}

// Scalar version of Contains
bool BloomFilter::Contains(hash_t hash) const {
  uint32_t alt_hash = static_cast<uint32_t>(hash >> 32);
  uint32_t block_idx = static_cast<uint32_t>(hash & block_mask_);

  Block &block = blocks_[block_idx];
  for (uint32_t i = 0; i < 8; i++) {
    uint32_t bit_idx = (alt_hash * K_SALTS[i]) >> 27;
    if (!util::BitUtil::Test(&block[i], bit_idx)) {
      return false;
    }
  }

  return true;
}
#endif

}  // namespace terrier::execution::sql
