#include "execution/sql/bloom_filter.h"

#include <limits>
#include <string>
#include <vector>

#include "execution/util/bit_util.h"
#include "execution/util/simd.h"
#include "loggers/execution_logger.h"

namespace noisepage::execution::sql {

BloomFilter::BloomFilter() noexcept {}  // NOLINT default constructor doesn't support noexcept

BloomFilter::BloomFilter(MemoryPool *memory) : memory_(memory) {}

BloomFilter::BloomFilter(MemoryPool *memory, uint32_t expected_num_elems) : BloomFilter() {
  Init(memory, expected_num_elems);
}

BloomFilter::~BloomFilter() {
  const auto num_bytes = GetNumBlocks() * sizeof(Block);
  if (memory_ != nullptr) memory_->Deallocate(blocks_, num_bytes);
}

void BloomFilter::Init(MemoryPool *memory, uint32_t expected_num_elems) {
  memory_ = memory;
  lazily_added_hashes_ = MemPoolVector<hash_t>(memory_);

  uint64_t num_bits = common::MathUtil::PowerOf2Ceil(BITS_PER_ELEMENT * expected_num_elems);
  uint64_t num_blocks = common::MathUtil::DivRoundUp(num_bits, sizeof(Block) * common::Constants::K_BITS_PER_BYTE);
  uint64_t num_bytes = num_blocks * sizeof(Block);
  blocks_ = reinterpret_cast<Block *>(memory->AllocateAligned(num_bytes, common::Constants::CACHELINE_SIZE, true));

  block_mask_ = static_cast<uint32_t>(num_blocks - 1);
  num_additions_ = 0;
}

void BloomFilter::Add(hash_t hash) {
  auto block_idx = static_cast<uint32_t>(hash & block_mask_);

  auto block = util::simd::Vec8().Load(blocks_[block_idx]);
  auto alt_hash = util::simd::Vec8(static_cast<uint32_t>(hash >> 32));
  auto salts = util::simd::Vec8().Load(SALTS);

  alt_hash *= salts;
  if constexpr (util::simd::Bitwidth::VALUE != 256) {
    // Make sure we're dealing with 32-bit values
    alt_hash &= util::simd::Vec8(std::numeric_limits<uint32_t>::max() - 1);
  }
  alt_hash >>= 27;

  auto masks = util::simd::Vec8(1) << alt_hash;

  block |= masks;

  block.Store(blocks_[block_idx]);

  num_additions_++;
}

bool BloomFilter::Contains(hash_t hash) const {
  auto block_idx = static_cast<uint32_t>(hash & block_mask_);

  auto block = util::simd::Vec8().Load(blocks_[block_idx]);
  auto alt_hash = util::simd::Vec8(static_cast<uint32_t>(hash >> 32));
  auto salts = util::simd::Vec8().Load(SALTS);

  alt_hash *= salts;
  if constexpr (util::simd::Bitwidth::VALUE != 256) {
    // Make sure we're dealing with 32-bit values
    alt_hash &= util::simd::Vec8(std::numeric_limits<uint32_t>::max() - 1);
  }
  alt_hash >>= 27;

  auto masks = util::simd::Vec8(1) << alt_hash;

  return block.AllBitsAtPositionsSet(masks);
}

uint64_t BloomFilter::GetTotalBitsSet() const {
  uint64_t count = 0;
  for (uint32_t i = 0; i < GetNumBlocks(); i++) {
    // Note that we process 64-bits at a time, thus we only need four iterations
    // over a block. We don't use SIMD here because this function isn't
    // performance-critical.
    const auto *const chunk = reinterpret_cast<const uint64_t *>(blocks_[i]);
    for (uint32_t j = 0; j < 4; j++) {
      count += util::BitUtil::CountPopulation(chunk[j]);
    }
  }
  return count;
}

std::string BloomFilter::DebugString() const {
  auto bits_per_elem = static_cast<double>(GetSizeInBits()) / GetNumAdditions();
  auto bit_set_prob = static_cast<double>(GetTotalBitsSet()) / GetSizeInBits();
  return fmt::format("Filter: {} elements, {} bits, {} bits/element, {} bits set (p={:.2f})", GetNumAdditions(),
                     GetSizeInBits(), bits_per_elem, GetTotalBitsSet(), bit_set_prob);
}

}  // namespace noisepage::execution::sql
