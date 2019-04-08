#include "storage/checkpoint_manager.h"
#include <vector>

#define NUM_RESERVED_COLUMNS 1u

namespace terrier::storage {
void CheckpointManager::Checkpoint(const SqlTable &table, const BlockLayout &layout) {
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
  auto end = table.end();
  while (it != end) {
    table.Scan(txn_, &it, columns);
    uint32_t num_tuples = columns->NumTuples();
    for (uint32_t off = 0; off < num_tuples; off++) {
      ProjectedColumns::RowView row = columns->InterpretAsRow(layout, off);
      out_.SerializeTuple(&row, row_buffer, layout);
    }
  }
  out_.Persist();
}

void CheckpointManager::Recover(const char *log_file_path) {
  BufferedTupleReader reader(log_file_path);

  while (reader.ReadNextBlock()) {
    CheckpointFilePage *page = reader.GetPage();
    // TODO(zhaozhe): check checksum here
    catalog::table_oid_t oid = page->GetTableOid();
    SqlTable *table = GetTable(oid);
    BlockLayout *layout = GetLayout(oid);

    ProjectedRow *row = nullptr;
    while ((row = reader.ReadNextRow()) != nullptr) {
      // loop through columns to deal with non-inlined varlens.
      for (uint16_t projection_list_idx = 0; projection_list_idx < row->NumColumns(); projection_list_idx++) {
        if (!row->IsNull(projection_list_idx)) {
          storage::col_id_t col = row->ColumnIds()[projection_list_idx];
          if (layout->IsVarlen(col)) {
            auto *entry = reinterpret_cast<VarlenEntry *>(row->AccessForceNotNull(projection_list_idx));
            if (!entry->IsInlined()) {
              uint32_t varlen_size = reader.ReadNextVarlenSize();
              byte *checkpoint_varlen_content = reader.ReadNextVarlen(varlen_size);
              byte *varlen_content = common::AllocationUtil::AllocateAligned(varlen_size);
              std::memcpy(varlen_content, checkpoint_varlen_content, varlen_size);
              *entry = VarlenEntry::Create(varlen_content, varlen_size, true);
            }
          }
        }
      }
      table->Insert(txn_, *row);
    }
  }
}

}  // namespace terrier::storage
