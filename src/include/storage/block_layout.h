#pragma once
#include <algorithm>
#include <functional>
#include <utility>
#include <vector>
#include "storage/storage_defs.h"

namespace terrier::storage {
/**
 * Stores metadata about the layout of a block.
 */
struct BlockLayout {
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
   * Number of columns.
   */
  const uint16_t NumColumns() const { return static_cast<uint16_t>(attr_sizes_.size()); }

  /**
   * attribute size at given col_id.
   */
  uint8_t AttrSize(col_id_t col_id) const { return attr_sizes_.at(!col_id); }

  /**
   * Tuple size.
   */
  const uint32_t TupleSize() const { return tuple_size_; }

  /**
   * Header size.
   */
  const uint32_t HeaderSize() const { return header_size_; }

  /**
   * Number of tuple slots in a block with this layout.
   */
  const uint32_t NumSlots() const { return num_slots_; }

 private:
  std::vector<uint8_t> attr_sizes_;
  // Cached values so that we don't have to iterate through attr_sizes_ every time.
  const uint32_t tuple_size_;
  // static_header_size is everything in the header that is not the bitmap (dependent in the number of slots)
  const uint32_t static_header_size_;
  const uint32_t num_slots_;
  // header is everything up to the first column
  const uint32_t header_size_;

 private:
  uint32_t ComputeTupleSize() const;
  uint32_t ComputeStaticHeaderSize() const;
  uint32_t ComputeNumSlots() const;
  uint32_t ComputeHeaderSize() const;
};
}  // namespace terrier::storage
