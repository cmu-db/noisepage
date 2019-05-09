#include <algorithm>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "common/object_pool.h"
#include "storage/garbage_collector.h"
#include "storage/checkpoint_manager.h"
#include "storage/sql_table.h"
#include "storage/storage_util.h"
#include "util/random_test_util.h"
#include "util/sql_transaction_test_util.h"
#include "util/storage_test_util.h"
#include "util/transaction_test_util.h"

#define CHECKPOINT_FILE_PREFIX "checkpoint_file_"
#define LOG_FILE_NAME "test.log"

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

  void StartLogging(uint32_t log_period_milli) {
    logging_ = true;
    log_thread_ = std::thread([log_period_milli, this] { LogThreadLoop(log_period_milli); });
  }

  void EndLogging() {
    logging_ = false;
    log_thread_.join();
    log_manager_->Shutdown();
  }
  
  void StartGC(transaction::TransactionManager *txn_manager, uint32_t gc_period_milli) {
    gc_ = new storage::GarbageCollector(txn_manager);
    run_gc_ = true;
    gc_thread_ = std::thread([gc_period_milli, this] { GCThreadLoop(gc_period_milli); });
  }
  
  void EndGC() {
    run_gc_ = false;
    gc_thread_.join();
    // Make sure all garbage is collected. This take 2 runs for unlink and deallocate
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    delete gc_;
  }
  
  
  // Members related to running gc.
  volatile bool run_gc_ = false;
  std::thread gc_thread_;
  storage::GarbageCollector *gc_;
  
  storage::CheckpointManager checkpoint_manager_{CHECKPOINT_FILE_PREFIX};
  transaction::TransactionManager *txn_manager_;
  std::default_random_engine generator_;
  storage::RecordBufferSegmentPool pool_{2000, 100};
  storage::BlockStore block_store_{100, 100};
  storage::LogManager *log_manager_;

 private:
  void CheckpointThreadLoop(uint32_t log_period_milli) {
    while (enable_checkpointing_) {
      transaction::TransactionContext *txn = txn_manager_->BeginTransaction();
      checkpoint_manager_.Process(txn, *table_, *schema_);
      txn_manager_->Commit(txn, StorageTestUtil::EmptyCallback, nullptr);
      delete txn;
      std::this_thread::sleep_for(std::chrono::milliseconds(log_period_milli));
    }
  }

  void LogThreadLoop(uint32_t log_period_milli) {
    while (logging_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(log_period_milli));
      log_manager_->Process();
    }
  }
  
  void GCThreadLoop(uint32_t gc_period_milli) {
    while (run_gc_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(gc_period_milli));
      gc_->PerformGarbageCollection();
    }
  }
  
  bool enable_checkpointing_;
  std::thread checkpoint_thread_;
  const storage::SqlTable *table_;
  const catalog::Schema *schema_;
  std::thread log_thread_;
  bool logging_;
};

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointRecoveryNoSeparateThread) {
  checkpoint_manager_.UnlinkCheckpointFiles();
  const uint32_t num_rows = 100000;
  const uint32_t num_columns = 3;
  int magic_seed = 13523777;
  double null_bias = 0.2;
  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  auto table_pair = tested.GenerateAndPopulateRandomTable(num_columns, true, &random_generator, num_rows, null_bias);

  storage::SqlTable *table = table_pair.first;
  catalog::Schema *schema = table_pair.second;
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
  std::pair<std::string, terrier::transaction::timestamp_t> checkpoint_pair =
      checkpoint_manager_.GetLatestCheckpointFilename();
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(1));
  checkpoint_manager_.StartRecovery(recovery_txn);
  checkpoint_manager_.RegisterTable(recovered_table);
  checkpoint_manager_.Recover(checkpoint_pair.first.c_str());
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
  delete table;
  delete schema;
}

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointRecoveryNoVarlen) {
  checkpoint_manager_.UnlinkCheckpointFiles();
  const uint32_t num_rows = 100;
  const uint32_t num_columns = 3;
  int magic_seed = 13523;
  double null_bias = 0.2;

  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  auto table_pair = tested.GenerateAndPopulateRandomTable(num_columns, false, &random_generator, num_rows, null_bias);

  storage::SqlTable *table = table_pair.first;
  catalog::Schema *schema = table_pair.second;
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
  std::pair<std::string, terrier::transaction::timestamp_t> checkpoint_pair =
      checkpoint_manager_.GetLatestCheckpointFilename();
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(1));
  checkpoint_manager_.StartRecovery(recovery_txn);
  checkpoint_manager_.RegisterTable(recovered_table);
  checkpoint_manager_.Recover(checkpoint_pair.first.c_str());
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
  delete table;
  delete schema;
}

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointRecoveryWithVarlen) {
  checkpoint_manager_.UnlinkCheckpointFiles();
  const uint32_t num_rows = 100;
  const uint32_t num_columns = 3;
  int magic_seed = 13523777;
  double null_bias = 0.2;

  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  auto table_pair = tested.GenerateAndPopulateRandomTable(num_columns, true, &random_generator, num_rows, null_bias);

  storage::SqlTable *table = table_pair.first;
  catalog::Schema *schema = table_pair.second;
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
  std::pair<std::string, terrier::transaction::timestamp_t> checkpoint_pair =
      checkpoint_manager_.GetLatestCheckpointFilename();
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(1));
  checkpoint_manager_.StartRecovery(recovery_txn);
  checkpoint_manager_.RegisterTable(recovered_table);
  checkpoint_manager_.Recover(checkpoint_pair.first.c_str());
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
  delete table;
  delete schema;
}

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointRecoveryWithHugeRow) {
  checkpoint_manager_.UnlinkCheckpointFiles();
  const uint32_t num_rows = 100;
  const uint32_t num_columns = 512;  // single row size is greater than the page size
  int magic_seed = 13523777;
  double null_bias = 0.2;

  // initialize test
  auto tested = RandomSqlTableTestObject();
  std::default_random_engine random_generator(magic_seed);
  auto table_pair = tested.GenerateAndPopulateRandomTable(num_columns, true, &random_generator, num_rows, null_bias);

  storage::SqlTable *table = table_pair.first;
  catalog::Schema *schema = table_pair.second;
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();

  // checkpoint
  StartCheckpointingThread(txn_manager, 50, table, schema);
  // Sleep for some time to ensure that the checkpoint thread has started at least one checkpoint. (Prevent racing)
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  EndCheckpointingThread();
  // read first run
  transaction::TransactionContext *scan_txn = txn_manager->BeginTransaction();
  std::vector<std::string> original_rows;
  StorageTestUtil::PrintAllRows(scan_txn, table, &original_rows);
  txn_manager->Commit(scan_txn, StorageTestUtil::EmptyCallback, nullptr);
  // recovery to another table
  std::pair<std::string, terrier::transaction::timestamp_t> checkpoint_pair =
      checkpoint_manager_.GetLatestCheckpointFilename();
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(1));
  checkpoint_manager_.StartRecovery(recovery_txn);
  checkpoint_manager_.RegisterTable(recovered_table);
  checkpoint_manager_.Recover(checkpoint_pair.first.c_str());
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
  delete table;
  delete schema;
}

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointAndLogRecoveryNoVarlen) {
  checkpoint_manager_.UnlinkCheckpointFiles();
  // First unlink log file and initialize log manager, to prevent existing log file affect the current test
  unlink(LOG_FILE_NAME);
  log_manager_ = new storage::LogManager{LOG_FILE_NAME, &pool_};
  const uint32_t num_rows = 100;
  const uint32_t num_columns = 10;
  // initialize test
  SqlLargeTransactionTestObject tested = SqlLargeTransactionTestObject::Builder()
                                             .SetMaxColumns(num_columns)
                                             .SetInitialTableSize(num_rows)
                                             .SetTxnLength(5)
                                             .SetUpdateSelectRatio({0.5, 0.5})
                                             .SetBlockStore(&block_store_)
                                             .SetBufferPool(&pool_)
                                             .SetGenerator(&generator_)
                                             .SetGcOn(true)
                                             .SetBookkeeping(true)
                                             .SetLogManager(log_manager_)
                                             .build();

  storage::SqlTable *table = tested.GetTable();
  const catalog::Schema *schema = tested.Schema();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();
  // checkpoint
  StartCheckpointingThread(txn_manager, 50, table, schema);
  // Sleep for some time to ensure that the checkpoint thread has started at least one checkpoint.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EndCheckpointingThread();

  // Run transactions to generate logs
  StartLogging(10);
  StartGC(tested.GetTxnManager(), 10);
  auto result = tested.SimulateOltp(100, 4);
  EndLogging();
  EndGC();

  // read first run
  transaction::TransactionContext *scan_txn = txn_manager->BeginTransaction();
  std::vector<std::string> original_rows;
  StorageTestUtil::PrintAllRows(scan_txn, table, &original_rows);
  txn_manager->Commit(scan_txn, StorageTestUtil::EmptyCallback, nullptr);
  // recovery to another table
  std::pair<std::string, terrier::transaction::timestamp_t> checkpoint_pair =
      checkpoint_manager_.GetLatestCheckpointFilename();
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  // Note: the logs hardcoded table_oid to 0, so we can only recover table with table_oid=0.
  // The correct way is actually to read from catalogs and initialize all required oids.
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(0));
  checkpoint_manager_.StartRecovery(recovery_txn);
  checkpoint_manager_.RegisterTable(recovered_table);
  checkpoint_manager_.Recover(checkpoint_pair.first.c_str());
  checkpoint_manager_.RecoverFromLogs(LOG_FILE_NAME, checkpoint_pair.second);
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
  unlink(LOG_FILE_NAME);
}

// NOLINTNEXTLINE
TEST_F(CheckpointTests, SimpleCheckpointAndLogRecoveryWithVarlen) {
  checkpoint_manager_.UnlinkCheckpointFiles();
  // First unlink log file and initialize log manager, to prevent existing log file affect the currrent test
  unlink(LOG_FILE_NAME);
  log_manager_ = new storage::LogManager{LOG_FILE_NAME, &pool_};
  const uint32_t num_rows = 100;
  const uint32_t num_columns = 10;
  // initialize test
  SqlLargeTransactionTestObject tested = SqlLargeTransactionTestObject::Builder()
                                             .SetMaxColumns(num_columns)
                                             .SetInitialTableSize(num_rows)
                                             .SetTxnLength(5)
                                             .SetUpdateSelectRatio({0.5, 0.5})
                                             .SetBlockStore(&block_store_)
                                             .SetBufferPool(&pool_)
                                             .SetGenerator(&generator_)
                                             .SetGcOn(true)
                                             .SetBookkeeping(true)
                                             .SetLogManager(log_manager_)
                                             .SetVarlenAllowed(true)
                                             .build();

  storage::SqlTable *table = tested.GetTable();
  const catalog::Schema *schema = tested.Schema();
  transaction::TransactionManager *txn_manager = tested.GetTxnManager();
  // checkpoint
  StartCheckpointingThread(txn_manager, 50, table, schema);
  // Sleep for some time to ensure that the checkpoint thread has started at least one checkpoint.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EndCheckpointingThread();

  // Run transactions to generate logs
  StartLogging(10);
  StartGC(tested.GetTxnManager(), 10);
  auto result = tested.SimulateOltp(100, 4);
  EndLogging();
  EndGC();

  // read first run
  transaction::TransactionContext *scan_txn = txn_manager->BeginTransaction();
  std::vector<std::string> original_rows;
  StorageTestUtil::PrintAllRows(scan_txn, table, &original_rows);
  txn_manager->Commit(scan_txn, StorageTestUtil::EmptyCallback, nullptr);
  // recovery to another table
  std::pair<std::string, terrier::transaction::timestamp_t> checkpoint_pair =
      checkpoint_manager_.GetLatestCheckpointFilename();
  transaction::TransactionContext *recovery_txn = txn_manager->BeginTransaction();
  storage::BlockStore block_store_{10000, 10000};
  storage::SqlTable *recovered_table = new storage::SqlTable(&block_store_, *schema, catalog::table_oid_t(0));
  checkpoint_manager_.StartRecovery(recovery_txn);
  checkpoint_manager_.RegisterTable(recovered_table);
  checkpoint_manager_.Recover(checkpoint_pair.first.c_str());
  checkpoint_manager_.RecoverFromLogs(LOG_FILE_NAME, checkpoint_pair.second);
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
  unlink(LOG_FILE_NAME);
}

}  // namespace terrier
