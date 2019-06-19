#include <unordered_map>
#include <vector>
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "settings/settings_callbacks.h"
#include "settings/settings_manager.h"
#include "storage/data_table.h"
#include "storage/garbage_collector_thread.h"
#include "storage/recovery/recovery_manager.h"
#include "storage/sql_table.h"
#include "storage/write_ahead_log/log_manager.h"
#include "transaction/transaction_manager.h"
#include "type/transient_value_factory.h"
#include "util/catalog_test_util.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

#define LOG_FILE_NAME "./test.log"

namespace terrier::storage {
class RecoveryTests : public TerrierTest {
 protected:
  storage::LogManager *log_manager_;
  storage::RecoveryManager *recovery_manager_;

  // Settings for log manager
  const uint64_t num_log_buffers_ = 100;
  const std::chrono::milliseconds log_serialization_interval_{10};
  const std::chrono::milliseconds log_persist_interval_{20};
  const uint64_t log_persist_threshold_ = (1 << 20);  // 1MB

  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool pool_{2000, 100};
  storage::BlockStore block_store_{100, 100};

  const std::chrono::milliseconds gc_period_{10};
  storage::GarbageCollectorThread *gc_thread_;

  void SetUp() override {
    // Unlink log file incase one exists from previous test iteration
    unlink(LOG_FILE_NAME);
        log_manager_ = new LogManager(LOG_FILE_NAME, num_log_buffers_, log_serialization_interval_,
        log_persist_interval_, log_persist_threshold_, &pool_);
    TerrierTest::SetUp();
  }

  void TearDown() override {
    // Delete log file
    unlink(LOG_FILE_NAME);
    DedicatedThreadRegistry::GetInstance().TearDown();
    TerrierTest::TearDown();
  }
};

// This test inserts some tuples with a single transaction into a single table. It then recreates the test table from the log, and verifies that this new table is the same as
// the original table
// NOLINTNEXTLINE
TEST_F(RecoveryTests, SingleTransactionRecoveryTest) {
  log_manager_->Start();

  const int num_inserts = 10;

  // Create original SQLTable
  auto col = catalog::Schema::Column("attribute", type::TypeId::INTEGER, false, catalog::col_oid_t(0));
  auto table_schema = catalog::Schema({col});
  auto *original_sql_table = new storage::SqlTable(&block_store_, table_schema, CatalogTestUtil::test_table_oid);
  auto tuple_initializer = original_sql_table->InitializerForProjectedRow({catalog::col_oid_t(0)}).first;

  // Insert tuples with a txn manager with logging enabled
  {
    transaction::TransactionManager txn_manager_{&pool_, true, log_manager_};
    auto *txn = txn_manager_.BeginTransaction();
    for (auto i = 0; i < num_inserts; i++) {
      auto *insert_redo =
          txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, tuple_initializer);
      auto *insert_tuple = insert_redo->Delta();
      *reinterpret_cast<int32_t *>(insert_tuple->AccessForceNotNull(0)) = i;
      original_sql_table->Insert(txn, insert_redo);
      EXPECT_TRUE(!txn->Aborted());
    }
    txn_manager_.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    log_manager_->PersistAndStop();
  }

  // Create recovery table and dummy catalog
  auto *recovered_sql_table = new storage::SqlTable(&block_store_, table_schema, CatalogTestUtil::test_table_oid);

  storage::RecoveryCatalog catalog;
  catalog[CatalogTestUtil::test_db_oid][CatalogTestUtil::test_table_oid] = recovered_sql_table;

  // Restart the transaction manager with logging disabled
  transaction::TransactionManager txn_manager_{&pool_, true, LOGGING_DISABLED};

  // Instantiate recovery manager, and recover the tables.
  recovery_manager_ = new RecoveryManager(LOG_FILE_NAME, &catalog, &txn_manager_);
  recovery_manager_->Recover();

  // Check we recovered all the original tuples
  auto original_tuples = StorageTestUtil::PrintRows(num_inserts, original_sql_table, original_sql_table->table_.layout, &txn_manager_);
  auto recovered_tuples = StorageTestUtil::PrintRows(num_inserts, recovered_sql_table, recovered_sql_table->table_.layout, &txn_manager_);

  EXPECT_EQ(num_inserts, original_tuples.size());
  EXPECT_EQ(num_inserts, recovered_tuples.size());
  EXPECT_EQ(original_tuples, recovered_tuples);

}

}  // namespace terrier::storage
