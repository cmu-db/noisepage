#include <string>
#include <random>
#include <iostream>
#include "common/object_pool.h"
#include "storage/storage_util.h"
#include "storage/sql_table.h"
#include "storage/checkpoint_manager.h"
#include "util/storage_test_util.h"
#include "util/transaction_test_util.h"
#include "util/random_test_util.h"


#define CHECKPOINT_FILE_PREFIX "checkpoint_"

namespace terrier {

struct CheckpointTests : public TerrierTest {};

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointNoVarlen) {
  const uint32_t num_rows = 100;
  const uint32_t num_columns = 3;
  int magic_seed = 13523;

  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  tested.GenerateRandomColumns(num_columns, false, &random_generator);
  tested.Create();
  tested.InsertRandomRows(num_rows, 0.2, &random_generator);

  storage::CheckpointManager manager(CHECKPOINT_FILE_PREFIX);
  storage::SqlTable *table = tested.GetTable();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  transaction::TransactionContext *txn = txn_manager->BeginTransaction();
  manager.StartCheckpoint(txn);
  manager.Checkpoint(*table, tested.GetLayout());
  manager.EndCheckpoint();
  txn_manager->Commit(txn, StorageTestUtil::EmptyCallback, nullptr);

}

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointWithVarlen) {
  const uint32_t num_rows = 100;
  const uint32_t num_columns = 3;
  int magic_seed = 13523;

  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  tested.GenerateRandomColumns(num_columns, true, &random_generator);
  tested.Create();
  tested.InsertRandomRows(num_rows, 0.2, &random_generator);

  storage::CheckpointManager manager(CHECKPOINT_FILE_PREFIX);
  storage::SqlTable *table = tested.GetTable();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  transaction::TransactionContext *txn = txn_manager->BeginTransaction();
  manager.StartCheckpoint(txn);
  manager.Checkpoint(*table, tested.GetLayout());
  manager.EndCheckpoint();
  txn_manager->Commit(txn, StorageTestUtil::EmptyCallback, nullptr);

}

}  // namespace terrier