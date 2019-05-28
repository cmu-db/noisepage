#pragma once

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/util/region.h"
#include "execution/util/region_containers.h"

namespace tpl::sql {

/**
 * Bloom Filter
 */
class BloomFilter {
  // The set of salt values we use to produce alternative hash values
  alignas(CACHELINE_SIZE) static constexpr const u32 kSalts[8] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
                                                                  0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

  static constexpr const u32 kBitsPerElement = 8;

 public:
  /// A block in this filter (i.e., the sizes of the bloom filter partitions)
  using Block = u32[8];

 public:
  /// Create an uninitialized bloom filter
  BloomFilter() noexcept;

  /// Create an uninitialized bloom filter with the given region allocator
  explicit BloomFilter(util::Region *region);

  /// Initialize this filter with the given size
  BloomFilter(util::Region *region, u32 num_elems);

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(BloomFilter);

  /// Initialize this BloomFilter with the given size
  void Init(util::Region *region, u32 num_elems);

  /// Add an element to the bloom filter
  void Add(hash_t hash);

  /// Check if the given element is contained in the filter
  /// \return True if the hash may be in the filter; false if definitely not
  bool Contains(hash_t hash) const;

  /// Return the size of this Bloom Filter in bytes
  u64 GetSizeInBytes() const { return sizeof(Block) * GetNumBlocks(); }

  /// Get the number of bits this Bloom Filter has
  u64 GetSizeInBits() const { return GetSizeInBytes() * kBitsPerByte; }

  /// Return the number of bits set in the Bloom Filter
  u64 GetTotalBitsSet() const;

 private:
  u32 GetNumBlocks() const { return block_mask_ + 1; }

 private:
  // The region allocator we use for all allocations
  util::Region *region_{nullptr};

  // The blocks. Note that this isn't allocated in a region and doesn't need to
  // be freed on destruction. That will be taken care of when the region gets
  // destroyed
  Block *blocks_{nullptr};

  // The mask used to determine which block a hash goes into
  u32 block_mask_{0};

  util::RegionVector<hash_t> lazily_added_hashes_;
};

#if 0
// ---------------------------------------------------------
// Implementation below
// ---------------------------------------------------------

inline void BloomFilter::Add_Slow(hash_t hash) {
  u32 block_idx = static_cast<u32>(hash & block_mask());
  Block &block = blocks_[block_idx];
  u32 alt_hash = static_cast<u32>(hash >> 32);
  for (u32 i = 0; i < 8; i++) {
    u32 bit_idx = (alt_hash * kSalts[i]) >> 27;
    util::BitUtil::Set(&block[i], bit_idx);
  }
}

inline bool BloomFilter::Contains_Slow(hash_t hash) const {
  u32 alt_hash = static_cast<u32>(hash >> 32);
  u32 block_idx = static_cast<u32>(hash & block_mask());

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

}  // namespace tpl::sql
