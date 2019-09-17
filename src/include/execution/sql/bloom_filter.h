#pragma once

#include <vector>

#include "common/constants.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "execution/sql/memory_pool.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::sql {

/**
 * A SIMD-optimized blocked bloom filter. The filter is composed of a contiguous
 * set of partitions, known as blocks. A block is 64-bytes, and thus, fits
 * within a cache line (in most systems). A block is further partitioned into
 * eight 32-bit chunks.
 *
 * Elements are inserted into the filter by selecting a block using bits from
 * the incoming hash value, computing eight derivative hash values from the
 * input hash, and setting a bit in each of the eight sub-partitions of the
 * block. This process is enhanced using SIMD instructions.
 */
class BloomFilter {
  // The set of salt values we use to produce alternative hash values
  alignas(common::Constants::CACHELINE_SIZE) static constexpr const uint32_t K_SALTS[8] = {
      0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

  static constexpr const uint32_t K_BITS_PER_ELEMENT = 8;

 public:
  /**
   * A block in this filter (i.e., the sizes_ of the bloom filter partitions)
   */
  using Block = uint32_t[8];

 public:
  /**
   * Create an uninitialized bloom filter. The bloom filter cannot be used
   * until a call to @em Init() is made.
   */
  BloomFilter();

  /**
   * Create an uninitialized bloom filter with the given memory pool
   * @param memory The allocator where this filter's memory is sourced from
   */
  explicit BloomFilter(MemoryPool *memory);

  /**
   * Create and initialize this filter with the given size @em num_elems
   * @param memory The allocator where this filter's memory is sourced from
   * @param num_elems The expected number of elements
   */
  BloomFilter(MemoryPool *memory, uint32_t num_elems);

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(BloomFilter);

  /**
   * Destructor
   */
  ~BloomFilter();

  /**
   * Initialize this bloom filter with the given size
   * @param memory The allocator where this filter's memory is sourced from
   * @param num_elems The expected number of elements
   */
  void Init(MemoryPool *memory, uint32_t num_elems);

  /**
   * Add an element to the bloom filter
   * @param hash The hash of the element to add
   */
  void Add(hash_t hash);

  /**
   * Check if the given element is contained in the filter
   * @param hash The hash value of the element to check
   * @return True if an element may be in the filter; false if definitely not
   */
  bool Contains(hash_t hash) const;

  /**
   * Return the size of the filter in bytes
   */
  uint64_t GetSizeInBytes() const { return sizeof(Block) * GetNumBlocks(); }

  /**
   * Return the number of bits in this filter
   */
  uint64_t GetSizeInBits() const { return GetSizeInBytes() * common::Constants::K_BITS_PER_BYTE; }

  /**
   * Return the number of set bits in this filter
   */
  uint64_t GetTotalBitsSet() const;

 private:
  uint32_t GetNumBlocks() const { return block_mask_ + 1; }

 private:
  // The memory allocator we use for all allocations
  MemoryPool *memory_{nullptr};

  // The blocks array
  Block *blocks_{nullptr};

  // The mask used to determine which block a hash goes into
  uint32_t block_mask_{0};

  // Temporary vector of lazily added hashes for bulk loading
  MemPoolVector<hash_t> lazily_added_hashes_{nullptr};
};

}  // namespace terrier::execution::sql
