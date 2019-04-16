#include "storage/storage_util.h"
#include <cstring>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "storage/projected_columns.h"
#include "storage/tuple_access_strategy.h"
#include "storage/undo_record.h"
namespace terrier::storage {

template <class RowType>
void StorageUtil::CopyWithNullCheck(const byte *const from, RowType *const to, const uint8_t size,
                                    const uint16_t projection_list_index) {
  if (from == nullptr)
    to->SetNull(projection_list_index);
  else
    std::memcpy(to->AccessForceNotNull(projection_list_index), from, size);
}

template void StorageUtil::CopyWithNullCheck<ProjectedRow>(const byte *, ProjectedRow *, uint8_t, uint16_t);
template void StorageUtil::CopyWithNullCheck<ProjectedColumns::RowView>(const byte *, ProjectedColumns::RowView *,
                                                                        uint8_t, uint16_t);

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
      TERRIER_ASSERT(delta_col_id != VERSION_POINTER_COLUMN_ID,
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

uint32_t StorageUtil::PadUpToSize(const uint8_t word_size, const uint32_t offset) {
  TERRIER_ASSERT((word_size & (word_size - 1)) == 0, "word_size should be a power of two.");
  // Because size is a power of two, mask is always all 1s up to the length of size.
  // example, size is 8 (1000), mask is (0111)
  uint32_t mask = word_size - 1;
  // This is equivalent to (offset + (size - 1)) / size, which always pads up as desired
  return (offset + mask) & (~mask);
}

std::vector<uint16_t> StorageUtil::ComputeBaseAttributeOffsets(const std::vector<uint8_t> &attr_sizes,
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

}  // namespace terrier::storage
