#pragma once
#include <vector>
#include "common/container/bitmap.h"
#include "common/macros.h"
#include "common/typedefs.h"
#include "storage/storage_util.h"

namespace terrier::storage {
/**
 * MaterializedColumns represents partial images of a collection of tuples, where columns from different
 * tuples are laid out continuously. This can be considered a collection of ProjectedRows, but optimized
 * for continuous column access like PAX. However, a ProjectedRow is almost always externally coupled to a known
 * tuple slot, so it is more compact in layout than MaterializedColumns, which has to also store the
 * TupleSlot information for each tuple.
 * ---------------------------------------------------------------------
 * | size | num_tuples | num_cols | col_id1 | col_id2 |      ...       |
 * ---------------------------------------------------------------------
 * | val1_offset | val2_offset | ... | TupleSlot_1 | TupleSlot_2 | ... |
 * ---------------------------------------------------------------------
 * | null-bitmap, col_id1 | val1, col_id1 | val2, col_id1 |    ...     |
 * ---------------------------------------------------------------------
 * | null-bitmap, col_id1 | val1, col_id2 | val2, col_id2 |    ...     |
 * ---------------------------------------------------------------------
 * | ...                                                               |
 * ---------------------------------------------------------------------
 */
class PACKED MaterializedColumns {
 public:
  MEM_REINTERPRETATION_ONLY(MaterializedColumns)
  uint32_t Size() const { return size_; }
  uint32_t NumTuples() const { return num_tuples_; }
  uint16_t NumColumns() const { return num_cols_; }
  col_id_t *ColumnIds() { return reinterpret_cast<col_id_t *>(varlen_contents_); }
  const col_id_t *ColumnIds() const { return reinterpret_cast<const col_id_t *>(varlen_contents_); }

  storage::TupleSlot *TupleSlots() {
    return StorageUtil::AlignedPtr<storage::TupleSlot>(AttrValueOffsets() + num_cols_);
  }

  common::RawBitmap *ColumnPresenceBitmap(uint16_t projection_list_index) {
    byte *column_start = reinterpret_cast<byte *>(this) + AttrValueOffsets()[projection_list_index];
    return reinterpret_cast<common::RawBitmap *>(column_start);
  }

  byte *ColumnStart(uint16_t projection_list_index) {
    // TODO(Tianyu): Just pad up to 8 bytes because we do not want to store block layout?
    // We should probably be consistent with what we do in blocks, which probably means modifying blocks
    // since I don't think replicating the block layout here sounds right.
    return StorageUtil::AlignedPtr(sizeof(uint64_t),
                                   ColumnPresenceBitmap(projection_list_index)
                                       + common::RawBitmap::SizeInBytes(num_tuples_));
  }

 private:
  friend class MaterializedColumnsInitializer;
  uint32_t size_;
  uint32_t num_tuples_;
  uint16_t num_cols_;
  byte varlen_contents_[0];

  uint32_t *AttrValueOffsets() { return StorageUtil::AlignedPtr<uint32_t>(ColumnIds() + num_cols_); }
  const uint32_t *AttrValueOffsets() const { return StorageUtil::AlignedPtr<const uint32_t>(ColumnIds() + num_cols_); }
};

class MaterializedColumnsInitializer {
 public:
  // TODO(Tianyu): num tuples or size in bytes?
  MaterializedColumnsInitializer(const BlockLayout &layout, std::vector<col_id_t> col_ids, uint32_t num_tuples);

  MaterializedColumns *Initialize(void *head) const;

  uint32_t MaterializedColumnsSize() const { return size_; }

  uint32_t NumTuples() const { return num_tuples_; }

  uint16_t NumColumns() const { return static_cast<uint16_t>(col_ids_.size()); }

  col_id_t ColId(uint16_t i) const { return col_ids_.at(i); }

 private:
  uint32_t size_ = 0;
  uint32_t num_tuples_;
  std::vector<col_id_t> col_ids_;
  std::vector<uint32_t> offsets_;
};
}  // namespace terrier::storage