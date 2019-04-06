#include "storage/checkpoint_manager.h"

#define NUM_RESERVED_COLUMNS 1u

namespace terrier::storage {
void CheckpointManager::Checkpoint(SqlTable &table, const storage::BlockLayout &layout) {
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
      out_.SerializeTuple(row, row_buffer, layout);
    }
  }
}
}  //namespace terrier::storage