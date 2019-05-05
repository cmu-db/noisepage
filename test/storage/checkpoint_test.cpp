#include <algorithm>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "common/object_pool.h"
#include "storage/checkpoint_manager.h"
#include "storage/sql_table.h"
#include "storage/storage_util.h"
#include "util/random_test_util.h"
#include "util/storage_test_util.h"
#include "util/transaction_test_util.h"

#define CHECKPOINT_FILE_PREFIX "checkpoint_file_"

namespace terrier {
class CheckpointTests : public TerrierTest {
 public:
  // Table and schema are temporary, for test purposes only. They should be fetched from catalogs.
  void StartCheckpointingThread(transaction::TransactionManager *txn_manager, uint32_t log_period_milli,
                                const storage::SqlTable *table, const catalog::Schema *schema) {
    enable_checkpointing_ = true;
    txn_manager_ = txn_manager;
    table_ = table;
    schema_ = schema;
    checkpoint_thread_ = std::thread([log_period_milli, this] { CheckpointThreadLoop(log_period_milli); });
  }

  void EndCheckpointingThread() {
    enable_checkpointing_ = false;
    checkpoint_thread_.join();
  }

  storage::CheckpointManager checkpoint_manager_{CHECKPOINT_FILE_PREFIX};
  std::thread checkpoint_thread_;
  transaction::TransactionManager *txn_manager_;
  bool enable_checkpointing_;

 private:
  void CheckpointThreadLoop(uint32_t log_period_milli) {
    while (enable_checkpointing_) {
      transaction::TransactionContext *txn = txn_manager_->BeginTransaction();
      checkpoint_manager_.Process(txn, *table_, *schema_);
      txn_manager_->Commit(txn, StorageTestUtil::EmptyCallback, nullptr);
      delete txn;
      STORAGE_LOG_DEBUG("Commited a checkpoint");
      std::this_thread::sleep_for(std::chrono::milliseconds(log_period_milli));
    }
  }
  const storage::SqlTable *table_;
  const catalog::Schema *schema_;
};

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointRecoveryNoSeparateThread) {
  const uint32_t num_rows = 100000;
  const uint32_t num_columns = 3;
  int magic_seed = 13523777;
  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  tested.GenerateRandomColumns(num_columns, true, &random_generator);
  tested.Create();
  tested.InsertRandomRows(num_rows, 0.2, &random_generator);

  storage::SqlTable *table = tested.GetTable();
  //  storage::BlockLayout layout = tested.GetLayout();
  catalog::Schema *schema = tested.GetSchema();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  // checkpoint
  transaction::TransactionContext *txn = txn_manager->BeginTransaction();
  checkpoint_manager_.Process(txn, *table, *schema);
  txn_manager->Commit(txn, StorageTestUtil::EmptyCallback, nullptr);
  // read first run
  transaction::TransactionContext *scan_txn = txn_manager->BeginTransaction();
  std::vector<std::string> original_rows;
  StorageTestUtil::PrintAllRows(scan_txn, table, &original_rows);
  txn_manager->Commit(scan_txn, StorageTestUtil::EmptyCallback, nullptr);
  // recovery to another table
  std::string latest_checkpoint_path = checkpoint_manager_.GetLatestCheckpointFilename();
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(1));
  checkpoint_manager_.StartRecovery(recovery_txn);
  checkpoint_manager_.RegisterTable(recovered_table);
  checkpoint_manager_.Recover(latest_checkpoint_path.c_str());
  checkpoint_manager_.EndRecovery();
  txn_manager->Commit(recovery_txn, StorageTestUtil::EmptyCallback, nullptr);
  // read recovered table
  transaction::TransactionContext *scan_txn_2 = txn_manager->BeginTransaction();
  std::vector<std::string> recovered_rows;
  StorageTestUtil::PrintAllRows(scan_txn_2, recovered_table, &recovered_rows);
  txn_manager->Commit(scan_txn_2, StorageTestUtil::EmptyCallback, nullptr);
  // compare
  std::vector<std::string> diff1, diff2;
  std::sort(original_rows.begin(), original_rows.end());
  std::sort(recovered_rows.begin(), recovered_rows.end());
  std::set_difference(original_rows.begin(), original_rows.end(), recovered_rows.begin(), recovered_rows.end(),
                      std::inserter(diff1, diff1.begin()));
  std::set_difference(recovered_rows.begin(), recovered_rows.end(), original_rows.begin(), original_rows.end(),
                      std::inserter(diff2, diff2.begin()));
  EXPECT_EQ(diff1.size(), 0);
  EXPECT_EQ(diff2.size(), 0);
  checkpoint_manager_.UnlinkCheckpointFiles();
  delete txn;
  delete recovered_table;
  delete scan_txn;
  delete scan_txn_2;
  delete recovery_txn;
}

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointRecoveryNoVarlen) {
  const uint32_t num_rows = 100000;
  const uint32_t num_columns = 3;
  int magic_seed = 13523;
  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  tested.GenerateRandomColumns(num_columns, false, &random_generator);
  tested.Create();
  tested.InsertRandomRows(num_rows, 0.2, &random_generator);

  storage::SqlTable *table = tested.GetTable();
  catalog::Schema *schema = tested.GetSchema();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  // checkpoint
  StartCheckpointingThread(txn_manager, 50, table, schema);
  // Sleep for some time to ensure that the checkpoint thread has started at least one checkpoint.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EndCheckpointingThread();
  // read first run
  transaction::TransactionContext *scan_txn = txn_manager->BeginTransaction();
  std::vector<std::string> original_rows;
  StorageTestUtil::PrintAllRows(scan_txn, table, &original_rows);
  txn_manager->Commit(scan_txn, StorageTestUtil::EmptyCallback, nullptr);
  // recovery to another table
  std::string latest_checkpoint_path = checkpoint_manager_.GetLatestCheckpointFilename();
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(1));
  checkpoint_manager_.StartRecovery(recovery_txn);
  checkpoint_manager_.RegisterTable(recovered_table);
  checkpoint_manager_.Recover(latest_checkpoint_path.c_str());
  checkpoint_manager_.EndRecovery();
  txn_manager->Commit(recovery_txn, StorageTestUtil::EmptyCallback, nullptr);
  // read recovered table
  transaction::TransactionContext *scan_txn_2 = txn_manager->BeginTransaction();
  std::vector<std::string> recovered_rows;
  StorageTestUtil::PrintAllRows(scan_txn_2, recovered_table, &recovered_rows);
  txn_manager->Commit(scan_txn_2, StorageTestUtil::EmptyCallback, nullptr);
  // compare
  std::vector<std::string> diff1, diff2;
  std::sort(original_rows.begin(), original_rows.end());
  std::sort(recovered_rows.begin(), recovered_rows.end());
  std::set_difference(original_rows.begin(), original_rows.end(), recovered_rows.begin(), recovered_rows.end(),
                      std::inserter(diff1, diff1.begin()));
  std::set_difference(recovered_rows.begin(), recovered_rows.end(), original_rows.begin(), original_rows.end(),
                      std::inserter(diff2, diff2.begin()));
  EXPECT_EQ(diff1.size(), 0);
  EXPECT_EQ(diff2.size(), 0);
  checkpoint_manager_.UnlinkCheckpointFiles();
  delete recovered_table;
  delete scan_txn;
  delete scan_txn_2;
  delete recovery_txn;
}

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointRecoveryWithVarlen) {
  const uint32_t num_rows = 1000;
  const uint32_t num_columns = 3;
  int magic_seed = 13523777;
  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  tested.GenerateRandomColumns(num_columns, true, &random_generator);
  tested.Create();
  tested.InsertRandomRows(num_rows, 0.2, &random_generator);

  storage::SqlTable *table = tested.GetTable();
  catalog::Schema *schema = tested.GetSchema();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  // checkpoint
  StartCheckpointingThread(txn_manager, 50, table, schema);
  // Sleep for some time to ensure that the checkpoint thread has started at least one checkpoint. (Prevent racing)
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EndCheckpointingThread();
  // read first run
  transaction::TransactionContext *scan_txn = txn_manager->BeginTransaction();
  std::vector<std::string> original_rows;
  StorageTestUtil::PrintAllRows(scan_txn, table, &original_rows);
  txn_manager->Commit(scan_txn, StorageTestUtil::EmptyCallback, nullptr);
  // recovery to another table
  std::string latest_checkpoint_path = checkpoint_manager_.GetLatestCheckpointFilename();
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(1));
  checkpoint_manager_.StartRecovery(recovery_txn);
  checkpoint_manager_.RegisterTable(recovered_table);
  checkpoint_manager_.Recover(latest_checkpoint_path.c_str());
  checkpoint_manager_.EndRecovery();
  txn_manager->Commit(recovery_txn, StorageTestUtil::EmptyCallback, nullptr);
  // read recovered table
  transaction::TransactionContext *scan_txn_2 = txn_manager->BeginTransaction();
  std::vector<std::string> recovered_rows;
  StorageTestUtil::PrintAllRows(scan_txn_2, recovered_table, &recovered_rows);
  txn_manager->Commit(scan_txn_2, StorageTestUtil::EmptyCallback, nullptr);
  // compare
  std::vector<std::string> diff1, diff2;
  std::sort(original_rows.begin(), original_rows.end());
  std::sort(recovered_rows.begin(), recovered_rows.end());
  std::set_difference(original_rows.begin(), original_rows.end(), recovered_rows.begin(), recovered_rows.end(),
                      std::inserter(diff1, diff1.begin()));
  std::set_difference(recovered_rows.begin(), recovered_rows.end(), original_rows.begin(), original_rows.end(),
                      std::inserter(diff2, diff2.begin()));
  EXPECT_EQ(diff1.size(), 0);
  EXPECT_EQ(diff2.size(), 0);
  checkpoint_manager_.UnlinkCheckpointFiles();
  delete recovered_table;
  delete scan_txn;
  delete scan_txn_2;
  delete recovery_txn;
}

}  // namespace terrier
