#include "execution/sql/execution_structures.h"
#include <iostream>
#include <memory>
#include <random>
#include "catalog/catalog_defs.h"
#include "catalog/catalog_sql_table.h"
#include "type/type_id.h"
#include "execution/util/bit_util.h"
#include "execution/util/common.h"
#include "execution/util/macros.h"

namespace tpl::sql {
ExecutionStructures::ExecutionStructures() {
  block_store_ = std::make_unique<BlockStore>(1000, 1000);
  buffer_pool_ =
      std::make_unique<RecordBufferSegmentPool>(100000, 100000);
  log_manager_ =
      std::make_unique<LogManager>("log_file.log", buffer_pool_.get());
  txn_manager_ = std::make_unique<TransactionManager>(
      buffer_pool_.get(), true, log_manager_.get());
  gc_ = std::make_unique<GarbageCollector>(txn_manager_.get());
  catalog_ = std::make_unique<Catalog>(txn_manager_.get(),
                                                block_store_.get());
  InitTestTables();
  InitTestSchemas();
  InitTestIndexes();
}

ExecutionStructures *ExecutionStructures::Instance() {
  static ExecutionStructures kInstance{};
  return &kInstance;
}

// TEST TABLES
/**
 * Enumeration to characterize the distribution of values in a given column
 */
enum class Dist : u8 { Uniform, Zipf_50, Zipf_75, Zipf_95, Zipf_99, Serial };

/**
 * Metadata about the data for a given column. Specifically, the type of the
 * column, the distribution of values, a min and max if appropriate.
 */
struct ColumnInsertMeta {
  const char *name;
  const TypeId type_;
  bool nullable;
  Dist dist;
  u64 min;
  u64 max;

  ColumnInsertMeta(const char *name, const TypeId type, bool nullable,
                   Dist dist, u64 min, u64 max)
      : name(name),
        type_(type),
        nullable(nullable),
        dist(dist),
        min(min),
        max(max) {}
};

/**
 * Metadata about data within a table. Specifically, the schema and number of
 * rows in the table.
 */
struct TableInsertMeta {
  const char *name;
  u32 num_rows;
  std::vector<ColumnInsertMeta> col_meta;

  TableInsertMeta(const char *name, u32 num_rows,
                  std::vector<ColumnInsertMeta> col_meta)
      : name(name), num_rows(num_rows), col_meta(std::move(col_meta)) {}
};

/**
 * This array configures each of the test tables. When the catalog is created,
 * it bootstraps itself with the tables in this array. Each able is configured
 * with a name, size, and schema. We also configure the columns of the table. If
 * you add a new table, set it up here.
 */

template <typename T>
T *CreateNumberColumnData(Dist dist, u32 num_vals, u64 min, u64 max) {
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

std::pair<byte *, u32 *> GenerateColumnData(const ColumnInsertMeta &col_meta,
                                            u32 num_rows) {
  // Create data
  byte *col_data = nullptr;
  switch (col_meta.type_) {
    case TypeId::BOOLEAN: {
      throw std::runtime_error("Implement me!");
    }
    case TypeId::SMALLINT: {
      col_data = reinterpret_cast<byte *>(CreateNumberColumnData<i16>(
          col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case TypeId::INTEGER: {
      col_data = reinterpret_cast<byte *>(CreateNumberColumnData<i32>(
          col_meta.dist, num_rows, col_meta.min, col_meta.max));
      break;
    }
    case TypeId::BIGINT:
    case TypeId::DECIMAL: {
      col_data = reinterpret_cast<byte *>(CreateNumberColumnData<i64>(
          col_meta.dist, num_rows, col_meta.min, col_meta.max));
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

void FillTable(const std::shared_ptr<terrier::catalog::SqlTableRW> &catalog_table,
               terrier::transaction::TransactionContext *txn,
               const TableInsertMeta &table_meta) {
  u32 batch_size = 10000;
  u32 num_batches = table_meta.num_rows / batch_size +
                    static_cast<u32>(table_meta.num_rows % batch_size != 0);
  u32 val_written = 0;
  auto *pri = catalog_table->GetPRI();
  auto *insert_buffer =
      terrier::common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
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
        if (table_meta.col_meta[k].nullable &&
            util::BitUtil::Test(column_data[k].second, j)) {
          insert->SetNull(offset);
        } else {
          byte *data = insert->AccessForceNotNull(offset);
          u32 elem_size =
              terrier::type::TypeUtil::GetTypeSize(table_meta.col_meta[k].type_);
          std::memcpy(data, column_data[k].first + j * elem_size, elem_size);
        }
      }
      catalog_table->GetSqlTable()->Insert(txn, *insert);
      val_written++;
    }

    // Free allocated buggers
    for (const auto &col_data : column_data) {
      std::free(col_data.first);
      std::free(col_data.second);
    }
  }
  delete[] insert_buffer;
  std::cout << "Create Table " << table_meta.name
            << " with number of tuples = " << val_written << std::endl;
}

void ExecutionStructures::InitTestTables() {
  // clang-format off
  std::vector<TableInsertMeta> insert_meta {
      // The empty table
      {"empty_table", 0,
       {{"colA", TypeId::INTEGER, false, Dist::Serial, 0, 0}}},

      // Table 1
      {"test_1", test1_size,
       {{"colA", TypeId::INTEGER, false, Dist::Serial, 0, 0},
        {"colB", TypeId::INTEGER, false, Dist::Uniform, 0, 9},
        {"colC", TypeId::INTEGER, false, Dist::Uniform, 0, 9999},
        {"colD", TypeId::INTEGER, false, Dist::Uniform, 0, 99999}}},

      // Table 2
      {"test_2", test2_size,
       {{"col1", TypeId::SMALLINT, false, Dist::Serial, 0, 0},
        {"col2", TypeId::INTEGER, true, Dist::Uniform, 0, 9},
        {"col3", TypeId::BIGINT, false, Dist::Uniform, 0, kDefaultVectorSize},
        {"col4", TypeId::INTEGER, true, Dist::Uniform, 0, 2 * kDefaultVectorSize}}}
  };

  auto *txn = txn_manager_->BeginTransaction();
  for (const auto & table_meta: insert_meta) {
    // Create Schema.
    std::vector<terrier::catalog::Schema::Column> cols;
    for (const auto & col_meta: table_meta.col_meta) {
      const terrier::catalog::col_oid_t col_oid(catalog_->GetNextOid());
      cols.emplace_back(col_meta.name, col_meta.type_, col_meta.nullable, col_oid);
    }
    terrier::catalog::Schema schema(cols);
    // Create Table.
    auto table_oid = catalog_->CreateTable(txn, terrier::catalog::DEFAULT_DATABASE_OID, table_meta.name, schema);
    auto catalog_table = catalog_->GetCatalogTable(terrier::catalog::DEFAULT_DATABASE_OID, table_oid);
    if (catalog_table != nullptr) {
      FillTable(catalog_table, txn, table_meta);
    }
  }
  // Commit the transaction.
  txn_manager_->Commit(txn, [](void*){}, nullptr);
}

void ExecutionStructures::InitTestSchemas() {
  // Build output1.tpl's final schema (simple seq_scan)
  auto catalog_table1 = catalog_->GetCatalogTable(terrier::catalog::DEFAULT_DATABASE_OID, "test_1");
  const terrier::catalog::Schema & schema1 = catalog_table1->GetSqlTable()->GetSchema();
  std::vector<terrier::catalog::Schema::Column> output_cols1{};
  output_cols1.emplace_back(schema1.GetColumns()[0]);
  output_cols1.emplace_back(schema1.GetColumns()[1]);

  std::unordered_map<uint32_t, uint32_t> offsets1{};
  offsets1[0] = 0;
  offsets1[1] = sql::ValUtil::GetSqlSize(schema1.GetColumns()[0].GetType());
  auto final_schema1 = std::make_shared<exec::FinalSchema>(output_cols1, offsets1);

  // Build output2.tpl's final schema (simple nested loop join)
  auto catalog_table2 = catalog_->GetCatalogTable(terrier::catalog::DEFAULT_DATABASE_OID, "test_2");
  const terrier::catalog::Schema & schema2 = catalog_table1->GetSqlTable()->GetSchema();
  std::vector<terrier::catalog::Schema::Column> output_cols2{};
  std::unordered_map<uint32_t, uint32_t> offsets2{};
  output_cols2.emplace_back(schema1.GetColumns()[0]);
  output_cols2.emplace_back(schema1.GetColumns()[1]);
  output_cols2.emplace_back(schema2.GetColumns()[0]);
  output_cols2.emplace_back(schema2.GetColumns()[1]);
  offsets2[0] = 0;
  offsets2[1] = sql::ValUtil::GetSqlSize(schema1.GetColumns()[0].GetType());
  offsets2[2] = offsets2[1] + sql::ValUtil::GetSqlSize(schema1.GetColumns()[1].GetType());
  offsets2[3] = offsets2[2] + sql::ValUtil::GetSqlSize(schema2.GetColumns()[0].GetType());
  auto final_schema2 = std::make_shared<exec::FinalSchema>(output_cols2, offsets2);

  test_plan_nodes_["output1.tpl"] = final_schema1;
  test_plan_nodes_["output2.tpl"] = final_schema2;
}


/**
 * Metadata about data within an index
 */

struct IndexColumn {
  const TypeId type_;
  bool nullable_;
  uint32_t table_col_idx_; // index in the original table

  IndexColumn(const TypeId type, bool nullable, uint32_t table_col_idx)
      : type_(type), nullable_(nullable), table_col_idx_(table_col_idx){}
};
struct IndexInsertMeta {
  const char * index_name;
  const char * table_name;
  std::vector<IndexColumn> cols;

  IndexInsertMeta(const char *index_name, const char *table_name, std::vector<IndexColumn> cols)
      : index_name(index_name), table_name(table_name), cols(std::move(cols)) {}
};

void FillIndex(const std::shared_ptr<terrier::catalog::CatalogIndex> & catalog_index,
    const std::shared_ptr<terrier::catalog::SqlTableRW> & catalog_table,
    terrier::transaction::TransactionContext* txn,
    const IndexInsertMeta & index_meta) {
  // Initialize the projected column
  const auto &sql_table = catalog_table->GetSqlTable();
  auto row_pri = catalog_table->GetPRI();
  auto index_pri = catalog_index->GetMetadata()->GetProjectedRowInitializer();

  byte * table_buffer = terrier::common::AllocationUtil::AllocateAligned(
      row_pri->ProjectedRowSize());
  byte * index_buffer = terrier::common::AllocationUtil::AllocateAligned(
      index_pri.ProjectedRowSize());
  auto table_pr = row_pri->InitializeRow(table_buffer);
  auto index_pr = index_pri.InitializeRow(index_buffer);
  u32 num_inserted = 0;
  for (const terrier::storage::TupleSlot & slot: *sql_table) {
    // Get table data
    sql_table->Select(txn, slot, table_pr);
    // Fill up the index data
    for (u32 col_idx = 0; col_idx < index_meta.cols.size(); col_idx++) {
      // Get the offset of this column in the table
      u16 table_offset = catalog_table->ColNumToOffset(index_meta.cols[col_idx].table_col_idx_);
      // Get the offset of this column in the index
      auto & index_col = catalog_index->GetMetadata()->GetKeySchema()[col_idx];
      u16 index_offset = static_cast<u16>(catalog_index->GetMetadata()->GetKeyOidToOffsetMap().at(index_col.GetOid()));
      // Check null and write bytes.
      if (index_col.IsNullable() && table_pr->IsNull(table_offset)) {
        index_pr->SetNull(index_offset);
      } else {
        byte* index_data = index_pr->AccessForceNotNull(index_offset);
        std::memcpy(index_data, table_pr->AccessForceNotNull(table_offset),
            terrier::type::TypeUtil::GetTypeSize(index_col.GetType()));
      }
    }
    // Insert tuple into the index
    catalog_index->GetIndex()->Insert(*index_pr, slot);
    num_inserted++;
  }
  // Cleanup
  delete [] table_buffer;
  delete [] index_buffer;
  std::cout << "Insert " << num_inserted << " tuples into " << index_meta.index_name << std::endl;
}


void ExecutionStructures::InitTestIndexes() {
  std::vector<IndexInsertMeta> index_metas = {
      // The empty table
      {"index_empty", "empty_table",
       {{TypeId::INTEGER, false, 0}}},

      // Table 1
      {"index_1", "test_1",
       {{TypeId::INTEGER, false, 0}}},

      // Table 2
      {"index_2", "test_2",
       {{TypeId::INTEGER, true, 1},
        {TypeId::SMALLINT, false, 0}}}
  };
  auto *txn = txn_manager_->BeginTransaction();

  for (const auto & index_meta: index_metas) {
    // Create Index Schema
    terrier::storage::index::IndexKeySchema schema;
    for (const auto & col_meta: index_meta.cols) {
      const terrier::catalog::indexkeycol_oid_t index_oid(catalog_->GetNextOid());
      schema.emplace_back(index_oid, col_meta.type_, col_meta.nullable_);
    }
    // Create Index
    auto index_oid = catalog_->CreateIndex(txn, terrier::storage::index::ConstraintType::DEFAULT, schema, index_meta.index_name);
    auto catalog_index = catalog_->GetCatalogIndex(index_oid);
    auto catalog_table = catalog_->GetCatalogTable(terrier::catalog::DEFAULT_DATABASE_OID, index_meta.table_name);
    catalog_index->SetTable(terrier::catalog::DEFAULT_DATABASE_OID, catalog_table->Oid());
    // Fill up the index
    FillIndex(catalog_index, catalog_table, txn, index_meta);
  }
  // Commit the transaction.
  txn_manager_->Commit(txn, [](void*){}, nullptr);
}


}  // namespace tpl::sql
