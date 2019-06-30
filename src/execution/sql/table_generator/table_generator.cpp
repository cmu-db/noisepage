#include "execution/sql/table_generator/table_generator.h"
#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "execution/util/bit_util.h"
#include "loggers/execution_logger.h"

namespace tpl::sql {

///////////////////////////////////////////////
/// Generating tables from files
//////////////////////////////////////////////

void TableGenerator::GenerateTableFromFile(const std::string &schema_file, const std::string &data_file) {
  table_reader.ReadTable(schema_file, data_file);
}

void TableGenerator::GenerateTPCHTables(const std::string &dir_name) {
  // TPCH table names;
  static const std::vector<std::string> tpch_tables{
      "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
  };
  for (const auto &table_name : tpch_tables) {
    table_reader.ReadTable(dir_name + "/" + table_name + ".schema", table_name + ".data");
  }
}

void TableGenerator::GenerateTablesFromDir(const std::string &dir_name) {
  // TODO(Amadou): Implement me!!!!
}

//////////////////////////////////////////////
/// Test Table Generation
//////////////////////////////////////////////

template <typename T>
T *TableGenerator::CreateNumberColumnData(Dist dist, u32 num_vals, u64 min, u64 max) {
  static u64 serial_counter = 0;
  auto *val = static_cast<T *>(malloc(sizeof(T) * num_vals));

  switch (dist) {
    case Dist::Uniform: {
      std::mt19937 generator{};
      std::uniform_int_distribution<T> distribution(static_cast<T>(min), static_cast<T>(max));

      for (u32 i = 0; i < num_vals; i++) {
        val[i] = distribution(generator);
      }

      break;
    }
    case Dist::Serial: {
      for (u32 i = 0; i < num_vals; i++) {
        val[i] = static_cast<T>(serial_counter);
        serial_counter++;
      }
      break;
    }
    default:
      throw std::runtime_error("Implement me!");
  }

  return val;
}

// Generate column data
std::pair<byte *, u32 *> TableGenerator::GenerateColumnData(const ColumnInsertMeta &col_meta, u32 num_rows) {
  // Create data
  byte *col_data = nullptr;
  switch (col_meta.type_) {
    case terrier::type::TypeId::BOOLEAN: {
      throw std::runtime_error("Implement me!");
    }
    case terrier::type::TypeId::SMALLINT: {
      col_data =
          reinterpret_cast<byte *>(CreateNumberColumnData<i16>(col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case terrier::type::TypeId::INTEGER: {
      col_data =
          reinterpret_cast<byte *>(CreateNumberColumnData<i32>(col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case terrier::type::TypeId::BIGINT:
    case terrier::type::TypeId::DECIMAL: {
      col_data =
          reinterpret_cast<byte *>(CreateNumberColumnData<i64>(col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    default: { throw std::runtime_error("Implement me!"); }
  }

  // Create bitmap
  u32 *null_bitmap = nullptr;
  TPL_ASSERT(num_rows != 0, "Cannot have 0 rows.");
  u64 num_words = util::BitUtil::Num32BitWordsFor(num_rows);
  null_bitmap = static_cast<u32 *>(malloc(num_words * sizeof(u32)));
  util::BitUtil::Clear(null_bitmap, num_rows);
  if (col_meta.nullable) {
    std::mt19937 generator;
    std::bernoulli_distribution coin(0.1);
    for (u32 i = 0; i < num_words; i++) {
      if (coin(generator)) util::BitUtil::Set(null_bitmap, i);
    }
  }

  return {col_data, null_bitmap};
}

// Fill a given table according to its metadata
void TableGenerator::FillTable(terrier::catalog::SqlTableHelper *catalog_table, const TableInsertMeta &table_meta) {
  u32 batch_size = 10000;
  u32 num_batches = table_meta.num_rows / batch_size + static_cast<u32>(table_meta.num_rows % batch_size != 0);
  auto *pri = catalog_table->GetPRI();
  auto *insert_buffer = terrier::common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
  auto *insert = pri->InitializeRow(insert_buffer);
  for (u32 i = 0; i < num_batches; i++) {
    std::vector<std::pair<byte *, u32 *>> column_data;

    // Generate column data for all columns
    u32 num_vals = std::min(batch_size, table_meta.num_rows - (i * batch_size));
    TPL_ASSERT(num_vals != 0, "Can't have empty columns.");
    for (const auto &col_meta : table_meta.col_meta) {
      column_data.emplace_back(GenerateColumnData(col_meta, num_vals));
    }

    // Insert into the table
    for (u32 j = 0; j < num_vals; j++) {
      for (u16 k = 0; k < column_data.size(); k++) {
        auto offset = catalog_table->ColNumToOffset(k);
        if (table_meta.col_meta[k].nullable && util::BitUtil::Test(column_data[k].second, j)) {
          insert->SetNull(offset);
        } else {
          byte *data = insert->AccessForceNotNull(offset);
          u32 elem_size = terrier::type::TypeUtil::GetTypeSize(table_meta.col_meta[k].type_);
          std::memcpy(data, column_data[k].first + j * elem_size, elem_size);
        }
      }
      catalog_table->GetSqlTable()->Insert(exec_ctx_->GetTxn(), *insert);
    }

    // Free allocated buffers
    for (const auto &col_data : column_data) {
      std::free(col_data.first);
      std::free(col_data.second);
    }
  }
  delete[] insert_buffer;
}

void TableGenerator::GenerateTestTables() {
  /**
   * This array configures each of the test tables. Each able is configured
   * with a name, size, and schema. We also configure the columns of the table. If
   * you add a new table, set it up here.
   */
  static const std::vector<TableInsertMeta> insert_meta{
      // The empty table
      {"empty_table", 0, {{"colA", terrier::type::TypeId::INTEGER, false, Dist::Serial, 0, 0}}},

      // Table 1
      {"test_1",
       test1_size,
       {{"colA", terrier::type::TypeId::INTEGER, false, Dist::Serial, 0, 0},
        {"colB", terrier::type::TypeId::INTEGER, false, Dist::Uniform, 0, 9},
        {"colC", terrier::type::TypeId::INTEGER, false, Dist::Uniform, 0, 9999},
        {"colD", terrier::type::TypeId::INTEGER, false, Dist::Uniform, 0, 99999}}},

      // Table 2
      {"test_2",
       test2_size,
       {{"col1", terrier::type::TypeId::SMALLINT, false, Dist::Serial, 0, 0},
        {"col2", terrier::type::TypeId::INTEGER, true, Dist::Uniform, 0, 9},
        {"col3", terrier::type::TypeId::BIGINT, false, Dist::Uniform, 0, kDefaultVectorSize},
        {"col4", terrier::type::TypeId::INTEGER, true, Dist::Uniform, 0, 2 * kDefaultVectorSize}}},

      // Empty table with two columns
      {"empty_table2",
       0,
       {{"colA", terrier::type::TypeId::INTEGER, false, Dist::Serial, 0, 0},
        {"colB", terrier::type::TypeId::BOOLEAN, false, Dist::Uniform, 0, 0}}},
  };
  for (const auto &table_meta : insert_meta) {
    // Create Schema.
    std::vector<terrier::catalog::Schema::Column> cols;
    for (const auto &col_meta : table_meta.col_meta) {
      const terrier::catalog::col_oid_t col_oid(exec_ctx_->GetAccessor()->GetNextOid());
      cols.emplace_back(col_meta.name, col_meta.type_, col_meta.nullable, col_oid);
    }
    terrier::catalog::Schema schema(cols);
    // Create Table.
    auto table_oid = exec_ctx_->GetAccessor()->CreateUserTable(table_meta.name, schema);
    auto catalog_table = exec_ctx_->GetAccessor()->GetUserTable(table_oid);
    if (catalog_table != nullptr) {
      FillTable(catalog_table, table_meta);
    }
    EXECUTION_LOG_INFO("Created Table {}", table_meta.name);
  }
}

void TableGenerator::FillIndex(const std::shared_ptr<terrier::catalog::CatalogIndex> &catalog_index,
                               terrier::catalog::SqlTableHelper *catalog_table, const IndexInsertMeta &index_meta) {
  // Initialize the projected column
  const auto &sql_table = catalog_table->GetSqlTable();
  auto row_pri = catalog_table->GetPRI();
  auto index_pri = catalog_index->GetMetadata()->GetProjectedRowInitializer();

  byte *table_buffer = terrier::common::AllocationUtil::AllocateAligned(row_pri->ProjectedRowSize());
  byte *index_buffer = terrier::common::AllocationUtil::AllocateAligned(index_pri.ProjectedRowSize());
  auto table_pr = row_pri->InitializeRow(table_buffer);
  auto index_pr = index_pri.InitializeRow(index_buffer);
  u32 num_inserted = 0;
  for (const terrier::storage::TupleSlot &slot : *sql_table) {
    // Get table data
    sql_table->Select(exec_ctx_->GetTxn(), slot, table_pr);
    // Fill up the index data
    for (u32 col_idx = 0; col_idx < index_meta.cols.size(); col_idx++) {
      // Get the offset of this column in the table
      u16 table_offset = catalog_table->ColNumToOffset(index_meta.cols[col_idx].table_col_idx_);
      // Get the offset of this column in the index
      auto &index_col = catalog_index->GetMetadata()->GetKeySchema()[col_idx];
      u16 index_offset = static_cast<u16>(catalog_index->GetMetadata()->GetKeyOidToOffsetMap().at(index_col.GetOid()));
      // Check null and write bytes.
      if (index_col.IsNullable() && table_pr->IsNull(table_offset)) {
        index_pr->SetNull(index_offset);
      } else {
        byte *index_data = index_pr->AccessForceNotNull(index_offset);
        std::memcpy(index_data, table_pr->AccessForceNotNull(table_offset),
                    terrier::type::TypeUtil::GetTypeSize(index_col.GetType()));
      }
    }
    // Insert tuple into the index
    catalog_index->GetIndex()->Insert(exec_ctx_->GetTxn(), *index_pr, slot);
    num_inserted++;
  }
  // Cleanup
  delete[] table_buffer;
  delete[] index_buffer;
  std::cout << "Insert " << num_inserted << " tuples into " << index_meta.index_name << std::endl;
}

void TableGenerator::InitTestIndexes() {
  /**
   * This array configures indexes. To add an index, modify this array
   */
  static const std::vector<IndexInsertMeta> index_metas = {
      // The empty table
      {"index_empty", "empty_table", {{terrier::type::TypeId::INTEGER, false, 0}}},

      // Table 1
      {"index_1", "test_1", {{terrier::type::TypeId::INTEGER, false, 0}}},

      // Table 2
      {"index_2", "test_2", {{terrier::type::TypeId::INTEGER, true, 1}, {terrier::type::TypeId::SMALLINT, false, 0}}}};

  for (const auto &index_meta : index_metas) {
    // Create Index Schema
    terrier::storage::index::IndexKeySchema schema;
    for (const auto &col_meta : index_meta.cols) {
      const terrier::catalog::indexkeycol_oid_t index_oid(exec_ctx_->GetAccessor()->GetNextOid());
      schema.emplace_back(index_oid, col_meta.type_, col_meta.nullable_);
    }
    // Create Index
    auto index_oid = exec_ctx_->GetAccessor()->CreateIndex(terrier::storage::index::ConstraintType::DEFAULT, schema,
                                                           index_meta.index_name);
    auto catalog_index = exec_ctx_->GetAccessor()->GetCatalogIndex(index_oid);
    auto catalog_table = exec_ctx_->GetAccessor()->GetUserTable(index_meta.table_name);
    catalog_index->SetTable(catalog_table->Oid());
    // Fill up the index
    FillIndex(catalog_index, catalog_table, index_meta);
  }
}
}  // namespace tpl::sql
