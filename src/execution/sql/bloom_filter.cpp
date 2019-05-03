#include "execution/sql/bloom_filter.h"

#include <limits>

#include "execution/util/bit_util.h"
#include "execution/util/simd.h"

namespace tpl::sql {

BloomFilter::BloomFilter() noexcept
    : region_(nullptr), blocks_(nullptr), block_mask_(0), lazily_added_hashes_(nullptr) {}

BloomFilter::BloomFilter(util::Region *region)
    : region_(region), blocks_(nullptr), block_mask_(0), lazily_added_hashes_(nullptr) {}

BloomFilter::BloomFilter(util::Region *region, u32 num_elems) : BloomFilter() { Init(region, num_elems); }

void BloomFilter::Init(util::Region *region, u32 num_elems) {
  region_ = region;
  lazily_added_hashes_ = util::RegionVector<hash_t>(region);

  u64 num_bits = util::MathUtil::PowerOf2Ceil(kBitsPerElement * num_elems);
  u64 num_blocks = util::MathUtil::DivRoundUp(num_bits, sizeof(Block) * kBitsPerByte);
  u64 num_bytes = num_blocks * sizeof(Block);
  blocks_ = reinterpret_cast<Block *>(region->Allocate(num_bytes, CACHELINE_SIZE));
  std::memset(blocks_, 0, num_bytes);

  block_mask_ = static_cast<u32>(num_blocks - 1);
}

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

}  // namespace tpl::sql
