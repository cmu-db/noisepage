#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "common/scoped_timer.h"
#include "main/db_main.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace noisepage {

class CatalogBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const benchmark::State &state) final {
    db_main_ = DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    auto *txn = txn_manager_->BeginTransaction();
    db_ = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  void TearDown(const benchmark::State &state) final { db_main_.reset(); }

  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;

  std::unique_ptr<DBMain> db_main_;

  catalog::db_oid_t db_;

  std::pair<catalog::table_oid_t, catalog::index_oid_t> AddUserTableAndIndex() {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

    // Create the column definition (no OIDs)
    std::vector<catalog::Schema::Column> cols;
    cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
    cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
                      parser::ConstantValueExpression(type::TypeId::INTEGER));
    auto tmp_schema = catalog::Schema(cols);

    const auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
    NOISEPAGE_ASSERT(table_oid != catalog::INVALID_TABLE_OID, "table creation should not fail");
    auto schema = accessor->GetSchema(table_oid);
    auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

    auto result UNUSED_ATTRIBUTE = accessor->SetTablePointer(table_oid, table);
    NOISEPAGE_ASSERT(result, "setting table pointer should not fail");
    auto idx_oid = AddIndex(accessor, table_oid, "test_table_idx", schema.GetColumn("id"));
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    return {table_oid, idx_oid};
  }

  std::pair<catalog::table_oid_t, std::vector<catalog::index_oid_t>> AddUserTableAndIndexes(
      const uint16_t num_indexes) {
    auto txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

    // Create the column definition (no OIDs)
    std::vector<catalog::Schema::Column> cols;
    cols.emplace_back("id", type::TypeId::INTEGER, false, parser::ConstantValueExpression(type::TypeId::INTEGER));
    cols.emplace_back("user_col_1", type::TypeId::INTEGER, false,
                      parser::ConstantValueExpression(type::TypeId::INTEGER));
    auto tmp_schema = catalog::Schema(cols);

    const auto table_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "test_table", tmp_schema);
    NOISEPAGE_ASSERT(table_oid != catalog::INVALID_TABLE_OID, "table creation should not fail");
    auto schema = accessor->GetSchema(table_oid);
    auto table = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), schema);

    auto result UNUSED_ATTRIBUTE = accessor->SetTablePointer(table_oid, table);
    NOISEPAGE_ASSERT(result, "setting table pointer should not fail");
    std::vector<catalog::index_oid_t> idx_oids;
    idx_oids.reserve(num_indexes);
    const auto &col = schema.GetColumn("id");
    for (uint16_t i = 0; i < num_indexes; i++) {
      idx_oids.push_back(AddIndex(accessor, table_oid, "test_table_idx_" + std::to_string(i), col));
    }
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    return {table_oid, idx_oids};
  }

  /**
   * Constructs an index on a specific column
   * @param accessor accessor to construct index in
   * @param table_oid table to consturct index on
   * @param name name of index
   * @param col col to create index on. Index col will also share the same name as this col
   * @return oid of index created
   */
  catalog::index_oid_t AddIndex(const std::unique_ptr<catalog::CatalogAccessor> &accessor,
                                catalog::table_oid_t table_oid, const std::string &index_name,
                                const catalog::Schema::Column &col) {
    std::vector<catalog::IndexSchema::Column> key_cols{catalog::IndexSchema::Column{
        col.Name(), type::TypeId::INTEGER, false, parser::ColumnValueExpression(db_, table_oid, col.Oid())}};
    auto index_schema = catalog::IndexSchema(key_cols, storage::index::IndexType::BPLUSTREE, true, true, false, true);
    const auto idx_oid = accessor->CreateIndex(accessor->GetDefaultNamespace(), table_oid, index_name, index_schema);
    NOISEPAGE_ASSERT(idx_oid != catalog::INVALID_INDEX_OID, "index creation should not fail");
    auto true_schema = accessor->GetIndexSchema(idx_oid);

    storage::index::IndexBuilder index_builder;
    index_builder.SetKeySchema(true_schema);
    auto index = index_builder.Build();
    bool result UNUSED_ATTRIBUTE = accessor->SetIndexPointer(idx_oid, index);
    NOISEPAGE_ASSERT(result, "setting index pointer should not fail");
    return idx_oid;
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetAccessor)(benchmark::State &state) {
  auto *txn = txn_manager_->BeginTransaction();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
    NOISEPAGE_ASSERT(accessor != nullptr, "getting accessor should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetDatabaseOid)(benchmark::State &state) {
  auto *txn = txn_manager_->BeginTransaction();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_oid UNUSED_ATTRIBUTE = catalog_->GetDatabaseOid(common::ManagedPointer(txn), "noisepage");
    NOISEPAGE_ASSERT(test_oid == db_, "getting oid should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetDatabaseCatalog)(benchmark::State &state) {
  auto *txn = txn_manager_->BeginTransaction();

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto dbc UNUSED_ATTRIBUTE = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn), db_);
    NOISEPAGE_ASSERT(dbc != nullptr, "getting accessor should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetIndex)(benchmark::State &state) {
  const auto oids UNUSED_ATTRIBUTE = AddUserTableAndIndex();

  auto *txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_index UNUSED_ATTRIBUTE = accessor->GetIndex(oids.second);
    NOISEPAGE_ASSERT(test_index != nullptr, "getting index should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetIndexOid)(benchmark::State &state) {
  const auto oids UNUSED_ATTRIBUTE = AddUserTableAndIndex();

  auto *txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_index UNUSED_ATTRIBUTE = accessor->GetIndexOid("test_table_idx");
    NOISEPAGE_ASSERT(oids.second == test_index, "getting index oid should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetIndexes)(benchmark::State &state) {
  const auto oids UNUSED_ATTRIBUTE = AddUserTableAndIndex();

  auto *txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_indexes UNUSED_ATTRIBUTE = accessor->GetIndexOids(oids.first);
    NOISEPAGE_ASSERT(!test_indexes.empty(), "getting index oids should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetIndexSchema)(benchmark::State &state) {
  const auto oids UNUSED_ATTRIBUTE = AddUserTableAndIndex();

  auto *txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_idx_schema UNUSED_ATTRIBUTE = accessor->GetIndexSchema(oids.second);
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetNamespaceOid)(benchmark::State &state) {
  auto txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);
  const auto ns_oid UNUSED_ATTRIBUTE = accessor->CreateNamespace("test_namespace");
  NOISEPAGE_ASSERT(ns_oid != catalog::INVALID_NAMESPACE_OID, "namespace creation should not fail");
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  txn = txn_manager_->BeginTransaction();
  accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_ns_oid UNUSED_ATTRIBUTE = accessor->GetNamespaceOid("test_namespace");
    NOISEPAGE_ASSERT(test_ns_oid == ns_oid, "namespace lookup should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetSchema)(benchmark::State &state) {
  const auto oids UNUSED_ATTRIBUTE = AddUserTableAndIndex();

  auto *txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_schema UNUSED_ATTRIBUTE = accessor->GetSchema(oids.first);
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetTable)(benchmark::State &state) {
  const auto oids UNUSED_ATTRIBUTE = AddUserTableAndIndex();

  auto *txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_table UNUSED_ATTRIBUTE = accessor->GetTable(oids.first);
    NOISEPAGE_ASSERT(test_table != nullptr, "table lookup should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetTableOid)(benchmark::State &state) {
  const auto oids UNUSED_ATTRIBUTE = AddUserTableAndIndex();

  auto *txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_table_oid UNUSED_ATTRIBUTE = accessor->GetTableOid("test_table");
    NOISEPAGE_ASSERT(test_table_oid == oids.first, "table oid lookup should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  state.SetItemsProcessed(state.iterations());
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(CatalogBenchmark, GetIndexObjects)(benchmark::State &state) {
  const auto num_indexes = 5;
  const auto oids UNUSED_ATTRIBUTE = AddUserTableAndIndexes(num_indexes);

  auto *txn = txn_manager_->BeginTransaction();
  auto accessor = catalog_->GetAccessor(common::ManagedPointer(txn), db_, DISABLED);

  // NOLINTNEXTLINE
  for (auto _ : state) {
    const auto test_indexes UNUSED_ATTRIBUTE = accessor->GetIndexes(oids.first);
    NOISEPAGE_ASSERT(!test_indexes.empty(), "getting index objects should not fail");
  }

  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  state.SetItemsProcessed(num_indexes * state.iterations());
}

// ----------------------------------------------------------------------------
// BENCHMARK REGISTRATION
// ----------------------------------------------------------------------------
// clang-format off

// Catalog benchmarks
BENCHMARK_REGISTER_F(CatalogBenchmark, GetAccessor)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetDatabaseOid)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetDatabaseCatalog)->Unit(benchmark::kNanosecond);

// CatalogAccessor benchmarks
BENCHMARK_REGISTER_F(CatalogBenchmark, GetIndex)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetIndexOid)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetIndexes)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetIndexSchema)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetNamespaceOid)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetSchema)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetTable)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetTableOid)->Unit(benchmark::kNanosecond);
BENCHMARK_REGISTER_F(CatalogBenchmark, GetIndexObjects)->Unit(benchmark::kNanosecond);
// clang-format on

}  // namespace noisepage
