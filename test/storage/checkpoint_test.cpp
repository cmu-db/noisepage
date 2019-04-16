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

#define CHECKPOINT_FILE_PREFIX "checkpoint_"

namespace terrier {

struct CheckpointTests : public TerrierTest {};

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointRecoveryNoVarlen) {
  const uint32_t num_rows = 103;
  const uint32_t num_columns = 3;
  int magic_seed = 13523;
  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  tested.GenerateRandomColumns(num_columns, false, &random_generator);
  tested.Create();
  tested.InsertRandomRows(num_rows, 0.2, &random_generator);

  storage::CheckpointManager manager(CHECKPOINT_FILE_PREFIX);
  storage::SqlTable *table = tested.GetTable();
  storage::BlockLayout layout = tested.GetLayout();
  catalog::Schema *schema = tested.GetSchema();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  // checkpoint
  transaction::TransactionContext *checkpoint_txn = txn_manager->BeginTransaction();
  manager.StartCheckpoint(checkpoint_txn);
  manager.Checkpoint(*table, *tested.GetSchema());
  manager.EndCheckpoint();
  txn_manager->Commit(checkpoint_txn, StorageTestUtil::EmptyCallback, nullptr);
  std::string checkpoint_path = manager.GetCheckpointFilePath(checkpoint_txn);
  // read first run
  transaction::TransactionContext *scan_txn = txn_manager->BeginTransaction();
  std::vector<std::string> original_rows;
  tested.PrintAllRows(scan_txn, table, &layout, &original_rows);
  txn_manager->Commit(scan_txn, StorageTestUtil::EmptyCallback, nullptr);
  // recovery to another table
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(1));
  manager.StartRecovery(recovery_txn);
  manager.RegisterTable(recovered_table, &layout);
  manager.Recover(checkpoint_path.c_str());
  manager.EndRecovery();
  txn_manager->Commit(recovery_txn, StorageTestUtil::EmptyCallback, nullptr);
  // read recovered table
  transaction::TransactionContext *scan_txn_2 = txn_manager->BeginTransaction();
  std::vector<std::string> recovered_rows;
  tested.PrintAllRows(scan_txn_2, recovered_table, &layout, &recovered_rows);
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
  unlink(checkpoint_path.c_str());
  delete recovered_table;
  delete checkpoint_txn;
  delete scan_txn;
  delete scan_txn_2;
  delete recovery_txn;
}

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointRecoveryWithVarlen) {
  const uint32_t num_rows = 103;
  const uint32_t num_columns = 3;
  int magic_seed = 13523777;
  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  tested.GenerateRandomColumns(num_columns, true, &random_generator);
  tested.Create();
  tested.InsertRandomRows(num_rows, 0.2, &random_generator);

  storage::CheckpointManager manager(CHECKPOINT_FILE_PREFIX);
  storage::SqlTable *table = tested.GetTable();
  storage::BlockLayout layout = tested.GetLayout();
  catalog::Schema *schema = tested.GetSchema();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  // checkpoint
  transaction::TransactionContext *checkpoint_txn = txn_manager->BeginTransaction();
  manager.StartCheckpoint(checkpoint_txn);
  manager.Checkpoint(*table, *tested.GetSchema());
  manager.EndCheckpoint();
  txn_manager->Commit(checkpoint_txn, StorageTestUtil::EmptyCallback, nullptr);
  std::string checkpoint_path = manager.GetCheckpointFilePath(checkpoint_txn);
  // read first run
  transaction::TransactionContext *scan_txn = txn_manager->BeginTransaction();
  std::vector<std::string> original_rows;
  tested.PrintAllRows(scan_txn, table, &layout, &original_rows);
  txn_manager->Commit(scan_txn, StorageTestUtil::EmptyCallback, nullptr);
  // recovery to another table
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(1));
  manager.StartRecovery(recovery_txn);
  manager.RegisterTable(recovered_table, &layout);
  manager.Recover(checkpoint_path.c_str());
  manager.EndRecovery();
  txn_manager->Commit(recovery_txn, StorageTestUtil::EmptyCallback, nullptr);
  // read recovered table
  transaction::TransactionContext *scan_txn_2 = txn_manager->BeginTransaction();
  std::vector<std::string> recovered_rows;
  tested.PrintAllRows(scan_txn_2, recovered_table, &layout, &recovered_rows);
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
  unlink(checkpoint_path.c_str());
  delete recovered_table;
  delete checkpoint_txn;
  delete scan_txn;
  delete scan_txn_2;
  delete recovery_txn;
}

}  // namespace terrier
