#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <vector>

#include "main/db_main.h"
#include "parser/expression/column_value_expression.h"
#include "portable_endian/portable_endian.h"
#include "storage/garbage_collector_thread.h"
#include "storage/index/compact_ints_key.h"
#include "storage/index/index_builder.h"
#include "storage/projected_row.h"
#include "storage/sql_table.h"
#include "test_util/catalog_test_util.h"
#include "test_util/data_table_test_util.h"
#include "test_util/random_test_util.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::storage::index {

class IndexBuilderTests : public TerrierTest {
 public:
  catalog::Schema table_schema_;
  catalog::IndexSchema index_schema_;

  std::default_random_engine generator_;
  const uint32_t num_threads_ = 4;

  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;

  // SqlTable
  storage::SqlTable *sql_table_;
  storage::ProjectedRowInitializer tuple_initializer_ =
      storage::ProjectedRowInitializer::Create(std::vector<uint16_t>{1}, std::vector<uint16_t>{1});

  common::WorkerPool thread_pool_{num_threads_, {}};

 protected:
  void SetUp() override {
    thread_pool_.Startup();
    db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseGCThread(true).SetRecordBufferSegmentSize(1e6).Build();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

    auto idxcol = catalog::Schema::Column(
        "attribute", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    auto nonidxcol = catalog::Schema::Column(
        "attribute2", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    StorageTestUtil::ForceOid(&(idxcol), catalog::col_oid_t(1));
    StorageTestUtil::ForceOid(&(nonidxcol), catalog::col_oid_t(2));
    table_schema_ = catalog::Schema({idxcol, nonidxcol});
    sql_table_ = new storage::SqlTable(db_main_->GetStorageLayer()->GetBlockStore(), table_schema_);
    tuple_initializer_ = sql_table_->InitializerForProjectedRow({catalog::col_oid_t(1)});

    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back("", type::TypeId::INTEGER, false,
                         parser::ColumnValueExpression(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID,
                                                       catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
    index_schema_ = catalog::IndexSchema(keycols, storage::index::IndexType::BWTREE, false, false, false, true);
  }
  void TearDown() override {
    thread_pool_.Shutdown();

    db_main_->GetTransactionLayer()->GetDeferredActionManager()->RegisterDeferredAction([=]() { delete sql_table_; });
  }
};

/**
 * This test creates multiple worker threads that all try to insert [0,num_inserts) as tuples in the table and into the
 * primary key index. At completion of the workload, only num_inserts_ txns should have committed with visible versions
 * in the index and table.
 */
// NOLINTNEXTLINE
TEST_F(IndexBuilderTests, NullTable) {

  auto index = (IndexBuilder().SetKeySchema(index_schema_).Build());

  auto txn = txn_manager_->BeginTransaction();
  EXPECT_NE(index, nullptr);

  std::vector<TupleSlot> values;
  index->ScanAscending(*txn, storage::index::ScanType::OpenBoth, 1, nullptr, nullptr, 0, &values);
  EXPECT_EQ(values.size(), 0);
  txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(IndexBuilderTests, OneTxnFullTable) {
  auto table_txn = txn_manager_->BeginTransaction();
  auto row_initializer = sql_table_->InitializerForProjectedRow({catalog::col_oid_t(1), catalog::col_oid_t(2)});
  uint32_t NUM_INSERTS = 1000;
  std::vector<uint32_t> keys;
  std::unordered_set<TupleSlot> reference;
  for(uint32_t i = 0; i < NUM_INSERTS; i++) {
    uint32_t key = random();
    uint32_t val = random();

    auto redo_record = table_txn->StageWrite(catalog::db_oid_t{1}, catalog::table_oid_t{1}, row_initializer);
    auto redo = redo_record->Delta();
    redo->Set<uint32_t, false>(0, key, false);
    redo->Set<uint32_t, false>(1, val, false);

    keys.push_back(key);
    reference.insert(sql_table_->Insert(common::ManagedPointer(table_txn), redo_record));
  }

  txn_manager_->Commit(table_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto index_build_txn = txn_manager_->BeginTransaction();

  auto index_builder = IndexBuilder().SetKeySchema(index_schema_).SetSqlTableAndTransactionContext(common::ManagedPointer(sql_table_), common::ManagedPointer(index_build_txn));
  auto index = index_builder.Build();
  index_builder.BulkInsert(index);


  txn_manager_->Commit(index_build_txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  auto index_scan_txn = txn_manager_->BeginTransaction();
  std::vector<TupleSlot> values;
  index->ScanAscending(*index_scan_txn, storage::index::ScanType::OpenBoth, 1, nullptr, nullptr, 0, &values);

  std::unordered_set<TupleSlot> result;
  for(TupleSlot t : values) {
    result.insert(t);
  }

  EXPECT_EQ(result, reference);

  txn_manager_->Commit(index_scan_txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}
}
