#include "storage/storage_util.h"

#include <cstring>
#include <unordered_map>
#include <vector>

#include "catalog/schema.h"
#include "execution/sql/vector_projection.h"
#include "storage/projected_columns.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"

namespace noisepage::storage {

template <class RowType>
void StorageUtil::CopyWithNullCheck(const byte *const from, RowType *const to, const uint16_t size,
                                    const uint16_t projection_list_index) {
  if (from == nullptr)
    to->SetNull(projection_list_index);
  else
    std::memcpy(to->AccessForceNotNull(projection_list_index), from, size);
}

template void StorageUtil::CopyWithNullCheck<ProjectedRow>(const byte *, ProjectedRow *, uint16_t, uint16_t);
template void StorageUtil::CopyWithNullCheck<ProjectedColumns::RowView>(const byte *, ProjectedColumns::RowView *,
                                                                        uint16_t, uint16_t);
template void StorageUtil::CopyWithNullCheck<execution::sql::VectorProjection::RowView>(
    const byte *, execution::sql::VectorProjection::RowView *, uint16_t, uint16_t);

void StorageUtil::CopyWithNullCheck(const byte *const from, const TupleAccessStrategy &accessor, const TupleSlot to,
                                    const col_id_t col_id) {
  if (from == nullptr)
    accessor.SetNull(to, col_id);
  else
    std::memcpy(accessor.AccessForceNotNull(to, col_id), from, accessor.GetBlockLayout().AttrSize(col_id));
}

template <class RowType>
void StorageUtil::CopyAttrIntoProjection(const TupleAccessStrategy &accessor, const TupleSlot from, RowType *const to,
                                         const uint16_t projection_list_offset) {
  col_id_t col_id = to->ColumnIds()[projection_list_offset];
  uint8_t attr_size = accessor.GetBlockLayout().AttrSize(col_id);
  byte *stored_attr = accessor.AccessWithNullCheck(from, col_id);
  CopyWithNullCheck(stored_attr, to, attr_size, projection_list_offset);
}

template void StorageUtil::CopyAttrIntoProjection<ProjectedRow>(const TupleAccessStrategy &, TupleSlot, ProjectedRow *,
                                                                uint16_t);
template void StorageUtil::CopyAttrIntoProjection<ProjectedColumns::RowView>(const TupleAccessStrategy &, TupleSlot,
                                                                             ProjectedColumns::RowView *, uint16_t);
template void StorageUtil::CopyAttrIntoProjection<execution::sql::VectorProjection::RowView>(
    const TupleAccessStrategy &, TupleSlot, execution::sql::VectorProjection::RowView *, uint16_t);

template <class RowType>
void StorageUtil::CopyAttrFromProjection(const TupleAccessStrategy &accessor, const TupleSlot to, const RowType &from,
                                         const uint16_t projection_list_offset) {
  col_id_t col_id = from.ColumnIds()[projection_list_offset];
  const byte *stored_attr = from.AccessWithNullCheck(projection_list_offset);
  CopyWithNullCheck(stored_attr, accessor, to, col_id);
}

template void StorageUtil::CopyAttrFromProjection<ProjectedRow>(const TupleAccessStrategy &, TupleSlot,
                                                                const ProjectedRow &, uint16_t);
template void StorageUtil::CopyAttrFromProjection<ProjectedColumns::RowView>(const TupleAccessStrategy &, TupleSlot,
                                                                             const ProjectedColumns::RowView &,
                                                                             uint16_t);
template void StorageUtil::CopyAttrFromProjection<execution::sql::VectorProjection::RowView>(
    const TupleAccessStrategy &, TupleSlot, const execution::sql::VectorProjection::RowView &, uint16_t);

template <class RowType>
void StorageUtil::ApplyDelta(const BlockLayout &layout, const ProjectedRow &delta, RowType *const buffer) {
  // the projection list in delta and buffer have to be sorted in the same way for this to work,
  // which should be guaranteed if both are constructed correctly using ProjectedRowInitializer,
  // (or copied from a valid ProjectedRow)
  uint16_t delta_i = 0, buffer_i = 0;
  while (delta_i < delta.NumColumns() && buffer_i < buffer->NumColumns()) {
    col_id_t delta_col_id = delta.ColumnIds()[delta_i], buffer_col_id = buffer->ColumnIds()[buffer_i];
    if (delta_col_id == buffer_col_id) {
      // Should apply changes
      NOISEPAGE_ASSERT(delta_col_id != VERSION_POINTER_COLUMN_ID,
                       "Output buffer should never return the version vector column.");
      uint8_t attr_size = layout.AttrSize(delta_col_id);
      StorageUtil::CopyWithNullCheck(delta.AccessWithNullCheck(delta_i), buffer, attr_size, buffer_i);
      delta_i++;
      buffer_i++;
    } else if (delta_col_id > buffer_col_id) {
      // buffer is behind
      buffer_i++;
    } else {
      // delta is behind
      delta_i++;
    }
  }
}

template void StorageUtil::ApplyDelta<ProjectedRow>(const BlockLayout &layout, const ProjectedRow &delta,
                                                    ProjectedRow *buffer);
template void StorageUtil::ApplyDelta<ProjectedColumns::RowView>(const BlockLayout &layout, const ProjectedRow &delta,
                                                                 ProjectedColumns::RowView *buffer);
template void StorageUtil::ApplyDelta<execution::sql::VectorProjection::RowView>(
    const BlockLayout &layout, const ProjectedRow &delta, execution::sql::VectorProjection::RowView *buffer);

uint32_t StorageUtil::PadUpToSize(const uint8_t word_size, const uint32_t offset) {
  NOISEPAGE_ASSERT((word_size & (word_size - 1)) == 0, "word_size should be a power of two.");
  // Because size is a power of two, mask is always all 1s up to the length of size.
  // example, size is 8 (1000), mask is (0111)
  uint32_t mask = word_size - 1;
  // This is equivalent to (offset + (size - 1)) / size, which always pads up as desired
  return (offset + mask) & (~mask);
}

std::vector<uint16_t> StorageUtil::ComputeBaseAttributeOffsets(const std::vector<uint16_t> &attr_sizes,
                                                               uint16_t num_reserved_columns) {
  // First compute {count_varlen, count_8, count_4, count_2, count_1}
  // Then {offset_varlen, offset_8, offset_4, offset_2, offset_1} is the inclusive scan of the counts
  std::vector<uint16_t> offsets;
  offsets.reserve(5);
  for (uint8_t i = 0; i < 5; i++) {
    offsets.emplace_back(0);
  }

  for (const auto &size : attr_sizes) {
    switch (size) {
      case VARLEN_COLUMN:
        offsets[1]++;
        break;
      case 8:
        offsets[2]++;
        break;
      case 4:
        offsets[3]++;
        break;
      case 2:
        offsets[4]++;
        break;
      case 1:
        break;
      default:
        throw std::runtime_error("unexpected switch case value");
    }
  }

  // reserved columns appear first
  offsets[0] = static_cast<uint16_t>(offsets[0] + num_reserved_columns);
  // reserved columns are size 8
  offsets[2] = static_cast<uint16_t>(offsets[2] - num_reserved_columns);

  // compute the offsets with an inclusive scan
  for (uint8_t i = 1; i < 5; i++) {
    offsets[i] = static_cast<uint16_t>(offsets[i] + offsets[i - 1]);
  }
  return offsets;
}

uint8_t StorageUtil::AttrSizeFromBoundaries(const std::vector<uint16_t> &boundaries, const uint16_t col_idx) {
  NOISEPAGE_ASSERT(boundaries.size() == NUM_ATTR_BOUNDARIES,
                   "Boudaries vector size should equal to number of boundaries");
  // Since the columns are sorted (DESC) by size, the boundaries denote the index boundaries between columns of size
  // 16|8|4|2|1. Iterate through boundaries until we find a boundary greater than the index.
  uint8_t shift;
  for (shift = 0; shift < NUM_ATTR_BOUNDARIES; shift++) {
    if (col_idx < boundaries[shift]) break;
  }
  NOISEPAGE_ASSERT(shift <= NUM_ATTR_BOUNDARIES, "Out-of-bounds attribute size");
  NOISEPAGE_ASSERT(shift >= 0, "Out-of-bounds attribute size");
  // The amount of boundaries we had to cross is how much we shift 16 (the max size) by
  return static_cast<uint8_t>(16U >> shift);
}

void StorageUtil::ComputeAttributeSizeBoundaries(const noisepage::storage::BlockLayout &layout, const col_id_t *col_ids,
                                                 const uint16_t num_cols, uint16_t *attr_boundaries) {
  int attr_size_index = 0;

  // Col ids ASC sorted order is also attribute size DESC sorted order
  for (uint16_t i = 0; i < num_cols; i++) {
    NOISEPAGE_ASSERT(i < (1 << 15), "Out-of-bounds index");

    int attr_size = layout.AttrSize(col_ids[i]);
    NOISEPAGE_ASSERT(attr_size <= (16 >> attr_size_index), "Out-of-order columns");
    NOISEPAGE_ASSERT(attr_size <= 16 && attr_size > 0, "Unexpected attribute size");
    // When we see a size that is less than our current boundary size, denote the end of the boundary
    while (attr_size < (16 >> attr_size_index)) {
      if (attr_size_index < (NUM_ATTR_BOUNDARIES - 1))
        attr_boundaries[attr_size_index + 1] = attr_boundaries[attr_size_index];
      attr_size_index++;
    }
    NOISEPAGE_ASSERT(attr_size == (16 >> attr_size_index), "Non-power of two attribute size");
    // At this point, this column's size is the same as the current boundary size, so we increment the number of cols
    // for this boundary
    if (attr_size_index < NUM_ATTR_BOUNDARIES) attr_boundaries[attr_size_index]++;
    NOISEPAGE_ASSERT(attr_size_index == NUM_ATTR_BOUNDARIES || attr_boundaries[attr_size_index] == i + 1,
                     "Inconsistent state on attribute bounds");
  }
}

std::vector<storage::col_id_t> StorageUtil::ProjectionListAllColumns(const storage::BlockLayout &layout) {
  std::vector<storage::col_id_t> col_ids(layout.NumColumns() - NUM_RESERVED_COLUMNS);
  // Add all of the column ids from the layout to the projection list
  // 0 is version vector so we skip it
  for (uint16_t col = NUM_RESERVED_COLUMNS; col < layout.NumColumns(); col++) {
    col_ids[col - NUM_RESERVED_COLUMNS] = storage::col_id_t(col);
  }
  return col_ids;
}

void StorageUtil::DeallocateVarlens(RawBlock *block, const TupleAccessStrategy &accessor) {
  const BlockLayout &layout = accessor.GetBlockLayout();
  for (col_id_t col : layout.Varlens()) {
    for (uint32_t offset = 0; offset < layout.NumSlots(); offset++) {
      TupleSlot slot(block, offset);
      if (!accessor.Allocated(slot)) continue;
      auto *entry = reinterpret_cast<VarlenEntry *>(accessor.AccessWithNullCheck(slot, col));
      // If entry is null here, the varlen entry is a null SQL value.
      if (entry != nullptr && entry->NeedReclaim()) delete[] entry->Content();
    }
  }
}

}  // namespace noisepage::storage
