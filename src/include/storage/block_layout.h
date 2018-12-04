#pragma once
#include <algorithm>
#include <functional>
#include <utility>
#include <vector>
#include "storage/storage_defs.h"

namespace terrier::storage {
// Internally we use the sign bit to represent if a column is varlen or not. Down to the implementation detail though,
// we always allocate 16 bytes for a varlen entry, with the first 8 bytes being the pointer to the value and following
// 4 bytes be the size of the varlen. There are 4 bytes of padding for alignment purposes.
#define VARLEN_COLUMN static_cast<uint8_t>(0x90)  // 16 with the first (most significant) bit set to 1
/**
 * Stores metadata about the layout of a block.
 */
struct BlockLayout {
  BlockLayout() = default;
  /**
   * Constructs a new block layout.
   * @warning The resulting column ids WILL be reordered and are not the same as the indexes given in the
   * attr_sizes, as the constructor applies optimizations based on sizes. It is up to the caller to then
   * associate these "column ids" with the right upper level concepts.
   *
   * @param attr_sizes vector of attribute sizes.
   */
  explicit BlockLayout(std::vector<uint8_t> attr_sizes);

  /**
   * @return number of columns.
   *
   */
  const uint16_t NumColumns() const { return static_cast<uint16_t>(attr_sizes_.size()); }

  /**
   * @param col_id the column id to check for
   * @return attribute size at given col_id.
   */
  uint8_t AttrSize(col_id_t col_id) const {
    // mask off the first bit as we use that to check for varlen
    return static_cast<uint8_t>(INT8_MAX & attr_sizes_.at(!col_id));
  }

  /**
   * @param col_id the column id to check for
   * @return if the given column id is varlen or not
   */
  bool IsVarlen(col_id_t col_id) const { return static_cast<int8_t>(AttrSize(col_id)) < 0; }

  // TODO(Tianyu): Eventually we may also want to get all the varlen columns associated with a block layout in a method
  // as well, but we can implement it then.

  /**
   * @return size, in bytes, of a full tuple in this block.
   */
  const uint32_t TupleSize() const { return tuple_size_; }

  /**
   * @return header size of the block.
   */
  const uint32_t HeaderSize() const { return header_size_; }

  /**
   * @return number of tuple slots in a block with this layout.
   */
  const uint32_t NumSlots() const { return num_slots_; }

 private:
  std::vector<uint8_t> attr_sizes_;
  // These fields below should be declared const but then that deletes the assignment operator for BlockLayout. With
  // const-only accessors we should be safe from making changes to a BlockLayout that would break stuff.

  // Cached values so that we don't have to iterate through attr_sizes_ every time.
  uint32_t tuple_size_;
  // static_header_size is everything in the header that is not the bitmap (dependent in the number of slots)
  uint32_t static_header_size_;
  uint32_t num_slots_;
  // header is everything up to the first column
  uint32_t header_size_;

 private:
  uint32_t ComputeTupleSize() const;
  uint32_t ComputeStaticHeaderSize() const;
  uint32_t ComputeNumSlots() const;
  uint32_t ComputeHeaderSize() const;
};
}  // namespace terrier::storage
