#include "storage/block_compactor.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "execution/exec/execution_context.h"
#include "catalog/catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "common/hash_util.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "storage/block_access_controller.h"
#include "storage/garbage_collector.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/index_builder.h"
#include "storage/recovery/disk_log_provider.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "storage/write_ahead_log/log_manager.h"
#include "test_util/catalog_test_util.h"
#include "test_util/sql_table_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

#define LOG_FILE_NAME "./test.log"

namespace terrier::storage {
class BlockCompactorTests : public TerrierTest {
 protected:
  std::default_random_engine generator_;

  // Original Components
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager;
  common::ManagedPointer<storage::LogManager> log_manager_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;

  void SetUp() override {
    // Unlink log file incase one exists from previous test iteration
    unlink(LOG_FILE_NAME);

    db_main_ = terrier::DBMain::Builder()
                   .SetLogFilePath(LOG_FILE_NAME)
                   .SetUseLogging(true)
                   .SetUseGC(true)
                   .SetUseGCThread(true)
                   .SetUseCatalog(true)
                   .Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    deferred_action_manager = db_main_->GetTransactionLayer()->GetDeferredActionManager();
    log_manager_ = db_main_->GetLogManager();
    block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
  }

  void TearDown() override {
    // Delete log file
    unlink(LOG_FILE_NAME);
  }

  catalog::IndexSchema DummyIndexSchema() {
    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back(
        "", type::TypeId::INTEGER, false,
        parser::ColumnValueExpression(catalog::db_oid_t(0), catalog::table_oid_t(0), catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
    return catalog::IndexSchema(keycols, storage::index::IndexType::BWTREE, true, true, false, true);
  }

  catalog::db_oid_t CreateDatabase(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::Catalog> catalog, const std::string &database_name) {
    auto db_oid = catalog->CreateDatabase(common::ManagedPointer(txn), database_name, true /* bootstrap */);
    EXPECT_TRUE(db_oid != catalog::INVALID_DATABASE_OID);
    return db_oid;
  }

  void DropDatabase(transaction::TransactionContext *txn, common::ManagedPointer<catalog::Catalog> catalog,
                    const catalog::db_oid_t db_oid) {
    EXPECT_TRUE(catalog->DeleteDatabase(common::ManagedPointer(txn), db_oid));
    EXPECT_FALSE(catalog->GetDatabaseCatalog(common::ManagedPointer(txn), db_oid));
  }

  catalog::table_oid_t CreateTable(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                   const catalog::namespace_oid_t ns_oid, const std::string &table_name) {
    auto col = catalog::Schema::Column(
        "attribute", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    auto table_schema = catalog::Schema(std::vector<catalog::Schema::Column>({col}));
    auto table_oid = db_catalog->CreateTable(common::ManagedPointer(txn), ns_oid, table_name, table_schema);
    EXPECT_TRUE(table_oid != catalog::INVALID_TABLE_OID);
    const auto catalog_schema = db_catalog->GetSchema(common::ManagedPointer(txn), table_oid);
    auto *table_ptr = new storage::SqlTable(block_store_, catalog_schema);
    EXPECT_TRUE(db_catalog->SetTablePointer(common::ManagedPointer(txn), table_oid, table_ptr));
    return table_oid;
  }

  void DropTable(transaction::TransactionContext *txn, common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                 const catalog::table_oid_t table_oid) {
    EXPECT_TRUE(db_catalog->DeleteTable(common::ManagedPointer(txn), table_oid));
  }

  catalog::index_oid_t CreateIndex(transaction::TransactionContext *txn,
                                   common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                   const catalog::namespace_oid_t ns_oid, const catalog::table_oid_t table_oid,
                                   const std::string &index_name) {
    auto index_schema = DummyIndexSchema();
    auto index_oid = db_catalog->CreateIndex(common::ManagedPointer(txn), ns_oid, index_name, table_oid, index_schema);
    EXPECT_TRUE(index_oid != catalog::INVALID_INDEX_OID);
    auto *index_ptr = storage::index::IndexBuilder().SetKeySchema(index_schema).Build();
    EXPECT_TRUE(db_catalog->SetIndexPointer(common::ManagedPointer(txn), index_oid, index_ptr));
    return index_oid;
  }

  void DropIndex(transaction::TransactionContext *txn, common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                 const catalog::index_oid_t index_oid) {
    EXPECT_TRUE(db_catalog->DeleteIndex(common::ManagedPointer(txn), index_oid));
  }

  catalog::namespace_oid_t CreateNamespace(transaction::TransactionContext *txn,
                                           common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                                           const std::string &namespace_name) {
    auto namespace_oid = db_catalog->CreateNamespace(common::ManagedPointer(txn), namespace_name);
    EXPECT_TRUE(namespace_oid != catalog::INVALID_NAMESPACE_OID);
    return namespace_oid;
  }

  void DropNamespace(transaction::TransactionContext *txn, common::ManagedPointer<catalog::DatabaseCatalog> db_catalog,
                     const catalog::namespace_oid_t ns_oid) {
    EXPECT_TRUE(db_catalog->DeleteNamespace(common::ManagedPointer(txn), ns_oid));
  }

  storage::RedoBuffer &GetRedoBuffer(transaction::TransactionContext *txn) { return txn->redo_buffer_; }

  storage::BlockLayout &GetBlockLayout(common::ManagedPointer<storage::SqlTable> table) const {
    return table->table_.layout_;
  }

  std::unique_ptr<execution::exec::ExecutionContext> MakeExecCtx(
      catalog::db_oid_t test_db_oid,
      transaction::TransactionContext *test_txn,
      common::ManagedPointer<catalog::CatalogAccessor> accessor,
      execution::exec::OutputCallback &&callback = nullptr,
      const planner::OutputSchema *schema = nullptr) {
    return std::make_unique<execution::exec::ExecutionContext>(test_db_oid, common::ManagedPointer(test_txn), callback, schema,
                                                    common::ManagedPointer(accessor));
  }

};

TEST_F(BlockCompactorTests, SimpleCompactionTest) {
  std::string database_name = "testdb";
  auto namespace_oid = catalog::postgres::NAMESPACE_DEFAULT_NAMESPACE_OID;
  std::string table_name = "foo";

  // Begin T0, create database, create table foo, and commit
  auto *txn0 = txn_manager_->BeginTransaction();
  auto db_oid = CreateDatabase(txn0, catalog_, database_name);
  auto db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn0), db_oid);
  auto table_oid = CreateTable(txn0, db_catalog, namespace_oid, table_name);
  txn_manager_->Commit(txn0, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Begin T1
  auto txn1 = txn_manager_->BeginTransaction();

  // With T1, insert into foo and commit. Even though T2 dropped foo, this operation should still succeed
  // because T1 got a snapshot before T2
  db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn1), db_oid);
  auto table_ptr = db_catalog->GetTable(common::ManagedPointer(txn1), table_oid);
  const auto &schema = db_catalog->GetSchema(common::ManagedPointer(txn1), table_oid);
  EXPECT_EQ(1, schema.GetColumns().size());
  EXPECT_EQ(type::TypeId::INTEGER, schema.GetColumn(0).Type());

  // Insert 5 tuples
  for (int32_t i = 0; i < 5; i++) {
    auto initializer = table_ptr->InitializerForProjectedRow({schema.GetColumn(0).Oid()});
    auto *redo_record = txn1->StageWrite(db_oid, table_oid, initializer);
    *reinterpret_cast<int32_t *>(redo_record->Delta()->AccessForceNotNull(0)) = i;
    table_ptr->Insert(common::ManagedPointer(txn1), redo_record);
  }
  txn_manager_->Commit(txn1, transaction::TransactionUtil::EmptyCallback, nullptr);

  int num_records = 0;
  for (auto it = table_ptr->begin(); it != table_ptr->end(); it++) {
    num_records++;
  }

  EXPECT_EQ(num_records, 5);

  auto txn2 = txn_manager_->BeginTransaction();
  db_catalog = catalog_->GetDatabaseCatalog(common::ManagedPointer(txn2), db_oid);
  table_ptr = db_catalog->GetTable(common::ManagedPointer(txn2), table_oid);

  num_records = 0;
  for (auto it = table_ptr->begin(); it != table_ptr->end(); it++) {
    if (num_records == 2) break;
    txn2->StageDelete(db_oid, table_oid, *it);
    table_ptr->Delete(common::ManagedPointer(txn2), *it);
    num_records++;
  }

  txn_manager_->Commit(txn2, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto txn3 = txn_manager_->BeginTransaction();
  std::unique_ptr<execution::exec::ExecutionContext> execCtx =
      MakeExecCtx(db_oid, txn3, common::ManagedPointer(
          catalog_->GetAccessor(common::ManagedPointer(txn3),
              db_oid)
      )); // Possible error -> Two pointers arent set (last 2 arguments not passed)
  txn_manager_->Commit(txn3, transaction::TransactionUtil::EmptyCallback, nullptr);

  col_id_t *col_oids = new col_id_t[1];
  col_oids[0] = (col_id_t)1;
  auto block = table_ptr->begin()->GetBlock();
  execution::exec::ExecutionContext *exec = execCtx.get();

  // Initialise block compactor and perform compaction
  storage::BlockCompactor compactor(exec, col_oids, table_name.c_str());
  compactor.PutInQueue(block);
  transaction::DeferredActionManager *deferred_action_manager_ptr = deferred_action_manager.Get();
  transaction::TransactionManager *txn_manager_ptr = txn_manager_.Get();
  compactor.ProcessCompactionQueue(deferred_action_manager_ptr, txn_manager_ptr);

  // Check for correctness of compaction
  auto txn4 = txn_manager_->BeginTransaction();
  auto initializer = table_ptr->InitializerForProjectedRow({schema.GetColumn(0).Oid()});
  byte *buffer = common::AllocationUtil::AllocateAligned(initializer.ProjectedRowSize());
  auto *read_row = initializer.InitializeRow(buffer);

  // 2, 3, 4 will be moved to the beginning of the block
  for (uint32_t i = 0; i < 3; i++) {
    storage::TupleSlot slot(block, i);
    bool visible = table_ptr->Select(common::ManagedPointer(txn4), slot, read_row);
    EXPECT_TRUE(visible);  // Should be filled after compaction
  }
}

}  // namespace terrier::storage