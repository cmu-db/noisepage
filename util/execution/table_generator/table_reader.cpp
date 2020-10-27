#include "execution/table_generator/table_reader.h"

#include <storage/index/index_builder.h>

#include <string>
#include <vector>

#include "csv/csv.hpp"  // NOLINT
#include "execution/sql/value.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

namespace noisepage::execution::sql {

uint32_t TableReader::ReadTable(const std::string &schema_file, const std::string &data_file) {
  uint32_t val_written = 0;
  // Read schema and create table and indexes
  SchemaReader schema_reader{};
  auto table_info = schema_reader.ReadTableInfo(schema_file);
  auto table_oid = CreateTable(table_info.get());

  // Init table projected row
  auto table = exec_ctx_->GetAccessor()->GetTable(table_oid);
  auto &table_schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  std::vector<catalog::col_oid_t> table_cols;
  for (const auto &col : table_schema.GetColumns()) {
    table_cols.emplace_back(col.Oid());
  }
  auto pri = table->InitializerForProjectedRow(table_cols);

  // Set table column offsets
  auto offset_map = table->ProjectionMapForOids(table_cols);
  std::vector<uint16_t> table_offsets;
  for (const auto &col_info : table_info->cols_) {
    const auto &col = table_schema.GetColumn(col_info.Name());
    table_offsets.emplace_back(offset_map[col.Oid()]);
  }

  // Create Indexes
  CreateIndexes(table_info.get(), table_oid);

  // Iterate through CSV file
  std::vector<std::string> col_names;
  for (const auto &col : table_info->cols_) {
    col_names.emplace_back(col.Name());
  }
  csv::CSVReader reader(data_file);
  for (csv::CSVRow &row : reader) {
    // Write table data
    auto *const redo = exec_ctx_->GetTxn()->StageWrite(exec_ctx_->DBOid(), table_oid, pri);
    uint16_t col_idx = 0;
    for (csv::CSVField &field : row) {
      auto col_offset = table_offsets[col_idx];
      auto col_type = table_info->cols_[col_idx].Type();
      WriteTableCol(redo->Delta(), col_offset, col_type, &field);
      col_idx++;
    }
    // Insert into sql table
    auto slot = table->Insert(exec_ctx_->GetTxn(), redo);
    val_written++;

    // Write index data
    for (auto &index_info : table_info->indexes_) {
      WriteIndexEntry(index_info.get(), redo->Delta(), table_offsets, slot);
    }
  }

  // Deallocate
  for (auto &index_info : table_info->indexes_) {
    delete[] reinterpret_cast<byte *>(index_info->index_pr_);
  }

  // Return
  return val_written;
}

catalog::table_oid_t TableReader::CreateTable(TableInfo *info) {
  catalog::Schema tmp_schema{info->cols_};
  auto table_oid = exec_ctx_->GetAccessor()->CreateTable(ns_oid_, info->table_name_, tmp_schema);
  auto &schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
  auto sql_table = new storage::SqlTable(common::ManagedPointer(store_), schema);
  exec_ctx_->GetAccessor()->SetTablePointer(table_oid, sql_table);
  return table_oid;
}

void TableReader::CreateIndexes(TableInfo *info, catalog::table_oid_t table_oid) {
  storage::index::IndexBuilder index_builder;
  for (auto &index_info : info->indexes_) {
    // Create index in catalog
    catalog::IndexSchema tmp_schema{index_info->cols_, storage::index::IndexType::BWTREE, false, false, false, false};
    auto index_oid = exec_ctx_->GetAccessor()->CreateIndex(ns_oid_, table_oid, index_info->index_name_, tmp_schema);
    auto &schema = exec_ctx_->GetAccessor()->GetIndexSchema(index_oid);

    // Build Index
    index_builder.SetKeySchema(schema);
    auto *index = index_builder.Build();
    exec_ctx_->GetAccessor()->SetIndexPointer(index_oid, index);
    index_info->index_ptr_ = exec_ctx_->GetAccessor()->GetIndex(index_oid);

    // Precompute offsets
    for (uint32_t i = 0; i < schema.GetColumns().size(); i++) {
      const auto &index_col_name = index_info->cols_[i].Name();
      auto &index_col = schema.GetColumn(index_col_name);
      index_info->offsets_.emplace_back(index->GetKeyOidToOffsetMap().at(index_col.Oid()));
    }

    // Create Projected Row
    auto &index_pri = index->GetProjectedRowInitializer();
    byte *index_buffer = common::AllocationUtil::AllocateAligned(index_pri.ProjectedRowSize());
    index_info->index_pr_ = index_pri.InitializeRow(index_buffer);
  }
}

void TableReader::WriteIndexEntry(IndexInfo *index_info, storage::ProjectedRow *table_pr,
                                  const std::vector<uint16_t> &table_offsets, const storage::TupleSlot &slot) {
  for (uint32_t index_col_idx = 0; index_col_idx < index_info->offsets_.size(); index_col_idx++) {
    // Get the offset of this column in the table
    uint16_t table_col_idx = index_info->index_map_[index_col_idx];
    uint16_t table_offset = table_offsets[table_col_idx];
    // Get the offset of this column in the index
    uint16_t index_offset = index_info->offsets_[index_col_idx];
    // Check null and write bytes.
    if (index_info->cols_[index_col_idx].Nullable() && table_pr->IsNull(table_offset)) {
      index_info->index_pr_->SetNull(index_offset);
    } else {
      byte *index_data = index_info->index_pr_->AccessForceNotNull(index_offset);
      uint8_t type_size = type::TypeUtil::GetTypeTrueSize(index_info->cols_[index_col_idx].Type());
      std::memcpy(index_data, table_pr->AccessForceNotNull(table_offset), type_size);
    }
  }
  // Insert into index
  index_info->index_ptr_->Insert(exec_ctx_->GetTxn(), *index_info->index_pr_, slot);
}

void TableReader::WriteTableCol(storage::ProjectedRow *insert_pr, uint16_t col_offset, type::TypeId type,
                                csv::CSVField *field) {
  if (*field == NULL_STRING) {
    insert_pr->SetNull(col_offset);
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
      auto val = field->get<double>();
      std::memcpy(insert_offset, &val, sizeof(double));
      break;
    }
    case type::TypeId::DATE: {
      auto val = sql::Date::FromString(field->get<std::string>());
      std::memcpy(insert_offset, &val, sizeof(uint32_t));
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
        auto content = common::AllocationUtil::AllocateAligned(content_size);
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

}  // namespace noisepage::execution::sql
