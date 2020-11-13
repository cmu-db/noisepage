#pragma once

#include <algorithm>
#include <utility>
#include <vector>

#include "storage/storage_defs.h"

namespace noisepage::storage {
// Used to retrieve the number of bytes an attribute actually occupies in memory. The size value stored in
// BlockLayout also has a bit denoting whether the attribute is variable-length and thus should be treated
// differently by the rest of the system.
constexpr uint16_t AttrSizeBytes(const uint16_t size) { return static_cast<uint16_t>((size)&INT16_MAX); }

/**
 * Stores metadata about the layout of a block.
 */
class BlockLayout {
 public:
  // TODO(Tianyu): This seems to be here only to make SqlTable::DataTableVersion's copy constructor happy.
  BlockLayout() = default;
  /**
   * Constructs a new block layout.
   * @warning The resulting column ids WILL be reordered and are not the same as the indexes given in the
   * attr_sizes, as the constructor applies optimizations based on sizes. It is up to the caller to then
   * associate these "column ids" with the right upper level concepts.
   *
   * @param attr_sizes vector of attribute sizes.
   */
  explicit BlockLayout(std::vector<uint16_t> attr_sizes);

  /**
   * @return number of columns.
   *
   */
  uint16_t NumColumns() const { return static_cast<uint16_t>(attr_sizes_.size()); }

  /**
   * @param col_id the column id to check for
   * @return attribute size at given col_id.
   */
  uint16_t AttrSize(col_id_t col_id) const {
    // mask off the first bit as we use that to check for varlen
    return AttrSizeBytes(attr_sizes_.at(col_id.UnderlyingValue()));
  }

  /**
   * @param col_id the column id to check for
   * @return if the given column id is varlen or not
   */
  bool IsVarlen(col_id_t col_id) const { return static_cast<int16_t>(attr_sizes_.at(col_id.UnderlyingValue())) < 0; }

  /**
   * @return all the varlen columns in the layout
   */
  const std::vector<col_id_t> &Varlens() const { return varlens_; }

  // TODO(Tianyu): Can probably store this like varlens to avoid computing every time.
  // TODO(Tianyu): The old test code has a util function that does this. Now that we are including this in the codebase
  // itself, we should replace the calls in test with this and delete that.
  /**
   * @return all the columns in the layout
   */
  std::vector<col_id_t> AllColumns() const {
    std::vector<col_id_t> result;
    for (uint16_t i = NUM_RESERVED_COLUMNS; i < attr_sizes_.size(); i++) result.emplace_back(i);
    return result;
  }

  /**
   * @return size, in bytes, of a full tuple in this block.
   */
  uint32_t TupleSize() const { return tuple_size_; }

  /**
   * @return header size of the block.
   */
  uint32_t HeaderSize() const { return header_size_; }

  /**
   * @return number of tuple slots in a block with this layout.
   */
  uint32_t NumSlots() const { return num_slots_; }

 private:
  std::vector<uint16_t> attr_sizes_;
  // keeps track of all the varlens to make iteration through all varlen columns faster
  std::vector<col_id_t> varlens_;
  // These fields below should be declared const but then that deletes the assignment operator for BlockLayout. With
  // const-only accessors we should be safe from making changes to a BlockLayout that would break stuff.

  // Cached values so that we don't have to iterate through attr_sizes_ every time.
  uint32_t tuple_size_;
  // static_header_size is everything in the header that is not dependent in the number of slots in the header
  uint32_t static_header_size_;
  uint32_t num_slots_;
  // header is everything up to the first column
  uint32_t header_size_;

  uint32_t ComputeTupleSize() const;
  // static header is the size of header that does not depend on the number of slots in the block
  uint32_t ComputeStaticHeaderSize() const;
  uint32_t ComputeNumSlots() const;
  uint32_t ComputeHeaderSize() const;
};
}  // namespace noisepage::storage
