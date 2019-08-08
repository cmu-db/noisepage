#include <storage/index/index_builder.h>
#include <string>
#include <vector>

#include "csv/csv.h"  // NOLINT
#include "execution/sql/table_generator/table_reader.h"
#include "execution/sql/value.h"

namespace terrier::execution::sql {

uint32_t TableReader::ReadTable(const std::string &schema_file, const std::string &data_file) {
  uint32_t val_written = 0;
  // Read schema and create table and indexes
  SchemaReader schema_reader{};
  auto table_info = schema_reader.ReadTableInfo(schema_file);
  auto table_oid = CreateTable(table_info.get());
  std::vector<catalog::index_oid_t> index_oids = CreateIndexes(table_info.get(), table_oid);

  // Init table projected row
  auto table = exec_ctx_->GetAccessor()->GetTable(table_oid);
  auto &table_schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  std::vector<catalog::col_oid_t> table_cols;
  for (const auto &col : table_schema.GetColumns()) {
    table_cols.emplace_back(col.Oid());
  }
  auto pri_map = table->InitializerForProjectedRow(table_cols);
  auto &pri = pri_map.first;

  // Init index prs
  std::vector<storage::ProjectedRow *> index_prs;
  for (const auto &index_oid : index_oids) {
    auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
    auto &index_pri = index->GetProjectedRowInitializer();
    byte *index_buffer = common::AllocationUtil::AllocateAligned(index_pri.ProjectedRowSize());
    auto index_pr = index_pri.InitializeRow(index_buffer);
    index_prs.emplace_back(index_pr);
  }

  // Set table column offsets
  auto &offset_map = pri_map.second;
  std::vector<u16> table_offsets;
  for (const auto &col_info : table_info->cols) {
    const auto &col = table_schema.GetColumn(col_info.Name());
    table_offsets.emplace_back(offset_map[col.Oid()]);
  }

  // Iterate through CSV file
  csv::CSVReader reader(data_file);
  for (csv::CSVRow &row : reader) {
    // Write table data
    auto *const redo = exec_ctx_->GetTxn()->StageWrite(exec_ctx_->DBOid(), table_oid, pri);
    uint16_t col_idx = 0;
    for (csv::CSVField &field : row) {
      auto col_offset = table_offsets[col_idx];
      auto col_type = table_info->cols[col_idx].Type();
      WriteTableCol(redo->Delta(), col_offset, col_type, &field);
      col_idx++;
    }
    // Insert into sql table
    auto slot = table->Insert(exec_ctx_->GetTxn(), redo);
    val_written++;

    // Write index data
    // TODO(Amadou): Turn this logic into a function
    for (uint32_t index_idx = 0; index_idx < index_oids.size(); index_idx++) {
      // Get the right catalog info and projected row
      auto index_oid = index_oids[index_idx];
      auto index = exec_ctx_->GetAccessor()->GetIndex(index_oid);
      auto index_schema = exec_ctx_->GetAccessor()->GetIndexSchema(index_oid);
      auto index_pr = index_prs[index_idx];
      for (u32 index_col_idx = 0; index_col_idx < index_schema.GetColumns().size(); index_col_idx++) {
        // Get the offset of this column in the table
        u16 table_col_idx = table_info->indexes[index_idx]->index_map[index_col_idx];
        u16 table_offset = table_offsets[table_col_idx];
        // Get the offset of this column in the index
        auto &index_col = index_schema.GetColumn(index_col_idx);
        u16 index_offset = static_cast<u16>(index->GetKeyOidToOffsetMap().at(index_col.Oid()));
        // Check null and write bytes.
        if (index_col.Nullable() && redo->Delta()->IsNull(table_offset)) {
          index_pr->SetNull(index_offset);
        } else {
          byte *index_data = index_pr->AccessForceNotNull(index_offset);
          uint8_t type_size = type::TypeUtil::GetTypeSize(index_col.Type()) & static_cast<uint8_t>(0x7f);
          std::memcpy(index_data, redo->Delta()->AccessForceNotNull(table_offset), type_size);
        }
      }
      // Insert into index
      index->Insert(exec_ctx_->GetTxn(), *index_pr, slot);
    }
  }

  // Deallocate
  for (auto &index_pr : index_prs) {
    delete[] reinterpret_cast<byte *>(index_pr);
  }

  // Return
  return val_written;
}

catalog::table_oid_t TableReader::CreateTable(TableInfo *info) {
  catalog::Schema tmp_schema{info->cols};
  auto table_oid = exec_ctx_->GetAccessor()->CreateTable(ns_oid_, info->table_name, tmp_schema);
  auto &schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  auto sql_table = new storage::SqlTable(store_, schema);
  exec_ctx_->GetAccessor()->SetTablePointer(table_oid, sql_table);
  return table_oid;
}

std::vector<catalog::index_oid_t> TableReader::CreateIndexes(TableInfo *info, catalog::table_oid_t table_oid) {
  std::vector<catalog::index_oid_t> results;
  storage::index::IndexBuilder index_builder;
  for (const auto &index_info : info->indexes) {
    catalog::IndexSchema tmp_schema{index_info->cols, false, false, false, false};
    auto index_oid = exec_ctx_->GetAccessor()->CreateIndex(ns_oid_, table_oid, index_info->index_name, tmp_schema);
    auto &schema = exec_ctx_->GetAccessor()->GetIndexSchema(index_oid);
    index_builder.SetOid(index_oid);
    index_builder.SetConstraintType(storage::index::ConstraintType::DEFAULT);
    index_builder.SetKeySchema(schema);
    auto *index = index_builder.Build();
    exec_ctx_->GetAccessor()->SetIndexPointer(index_oid, index);
    results.emplace_back(index_oid);
  }
  return results;
}

void TableReader::WriteTableCol(storage::ProjectedRow *insert_pr, uint16_t col_offset, type::TypeId type,
                                csv::CSVField *field) {
  if (*field == null_string) {
    insert_pr->SetNull(col_offset);
    std::cout << "setting null" << std::endl;
    return;
  }
  byte *insert_offset = insert_pr->AccessForceNotNull(col_offset);
  switch (type) {
    case type::TypeId::TINYINT: {
      auto val = field->get<int8_t>();
      std::memcpy(insert_offset, &val, sizeof(int8_t));
      break;
    }
    case type::TypeId::SMALLINT: {
      auto val = field->get<int16_t>();
      std::memcpy(insert_offset, &val, sizeof(int16_t));
      break;
    }
    case type::TypeId::INTEGER: {
      auto val = field->get<int32_t>();
      std::memcpy(insert_offset, &val, sizeof(int32_t));
      break;
    }
    case type::TypeId::BIGINT: {
      auto val = field->get<int64_t>();
      std::memcpy(insert_offset, &val, sizeof(int64_t));
      break;
    }
    case type::TypeId::DECIMAL: {
      auto val = field->get<f64>();
      std::memcpy(insert_offset, &val, sizeof(f64));
      break;
    }
    case type::TypeId::DATE: {
      auto val = sql::ValUtil::StringToDate(field->get<std::string>());
      std::memcpy(insert_offset, &val.int_val, sizeof(u32));
      break;
    }
    case type::TypeId::VARCHAR: {
      auto val = field->get<std::string_view>();
      auto content_size = static_cast<uint32_t>(val.size());
      if (content_size <= storage::VarlenEntry::InlineThreshold()) {
        *reinterpret_cast<storage::VarlenEntry *>(insert_offset) =
            storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *>(val.data()), content_size);
      } else {
        // TODO(Amadou): Use execCtx allocator
        auto content = reinterpret_cast<byte *>(common::AllocationUtil::AllocateAligned(content_size));
        std::memcpy(content, val.data(), content_size);
        *reinterpret_cast<storage::VarlenEntry *>(insert_offset) =
            storage::VarlenEntry::Create(content, content_size, true);
      }
      break;
    }
    default:
      UNREACHABLE("Unsupported type. Add it here first!!!");
  }
}

}  // namespace terrier::execution::sql
