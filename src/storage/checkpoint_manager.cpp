#include "storage/checkpoint_manager.h"

#define NUM_RESERVED_COLUMNS 1u

namespace terrier::storage {
void CheckpointManager::Checkpoint(DataTable &table, const storage::BlockLayout &layout) {
    std::vector<storage::col_id_t> all_col(layout.NumColumns() - NUM_RESERVED_COLUMNS);
    // Add all of the column ids from the layout to the projection list
    // 0 is version vector so we skip it
    for (uint16_t col = NUM_RESERVED_COLUMNS; col < layout.NumColumns(); col++) {
      all_col[col - NUM_RESERVED_COLUMNS] = storage::col_id_t(col);
    }

  // TODO(Mengyang): should calculate number of tuple to fit in buffer_size_
  // now I only use a magic number here
  uint32_t max_tuples = 10;

  ProjectedColumnsInitializer column_initializer(layout, all_col, max_tuples);
  auto *scan_buffer = common::AllocationUtil::AllocateAligned(column_initializer.ProjectedColumnsSize());
  ProjectedColumns *columns = column_initializer.Initialize(scan_buffer);

  ProjectedRowInitializer row_initializer(layout, all_col);
  auto *redo_buffer = common::AllocationUtil::AllocateAligned(row_initializer.ProjectedRowSize());
  ProjectedRow *row_buffer = row_initializer.InitializeRow(redo_buffer);

  auto it = table.begin();
  while (it != table.end()) {
    table.Scan(txn_, &it, columns);
    uint32_t num_tuples = columns->NumTuples();
    for (uint32_t off = 0; off < num_tuples; off++) {
      ProjectedColumns::RowView row = columns->InterpretAsRow(layout, off);
      for (uint16_t projection_list_idx = 0; projection_list_idx < row.NumColumns(); projection_list_idx++) {

        if (row.IsNull(projection_list_idx)) {
          row_buffer->SetNull(projection_list_idx);
        } else {
          row_buffer->SetNotNull(projection_list_idx);
          storage::col_id_t col = row.ColumnIds()[projection_list_idx];
          if (layout.IsVarlen(col)) {
            VarlenEntry *varlen_entry = reinterpret_cast<VarlenEntry *>(row.AccessForceNotNull(projection_list_idx));
            if (varlen_entry->IsInlined()) {
              std::memcpy(row_buffer->AccessForceNotNull(projection_list_idx), varlen_entry, sizeof(VarlenEntry));
            } else {
              // use the content_ field in VarlenEntry as the offset to the varlen file.
              uint32_t size = varlen_entry->Size();
              *reinterpret_cast<VarlenEntry *>(row_buffer->AccessForceNotNull(projection_list_idx)) = VarlenEntry::CreateCheckpoint(varlen_offset_, size);
              SerializeVarlen(varlen_entry);
            }
          } else {
            std::memcpy(row_buffer->AccessForceNotNull(projection_list_idx),
                        row.AccessForceNotNull(projection_list_idx),
                        layout.AttrSize(col));
          }
        }
      }
      SerializeRow(row_buffer);
    }
  }
}
}  //namespace terrier::storage