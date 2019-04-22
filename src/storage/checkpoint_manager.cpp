#include "storage/checkpoint_manager.h"
#include <vector>

#define NUM_RESERVED_COLUMNS 1u

namespace terrier::storage {
void CheckpointManager::Checkpoint(const SqlTable &table, const catalog::Schema &schema) {
  std::vector<catalog::col_oid_t> all_col(schema.GetColumns().size());
  uint16_t col_idx = 0;
  for (const catalog::Schema::Column &column : schema.GetColumns()) {
    all_col[col_idx] = column.GetOid();
    col_idx++;
  }

  auto row_pair = table.InitializerForProjectedRow(all_col);
  auto *buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  ProjectedRow *row_buffer = row_pair.first.InitializeRow(buffer);

  for (auto &slot : table) {
    if (table.Select(txn_, slot, row_buffer)) {
      out_.SerializeTuple(row_buffer, schema, row_pair.second);
    }
  }
  out_.Persist();
  delete[] buffer;
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
