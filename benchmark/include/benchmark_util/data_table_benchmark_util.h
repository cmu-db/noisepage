#pragma once
#include <algorithm>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "metrics/metrics_thread.h"
#include "storage/data_table.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier {

class LargeDataTableBenchmarkObject;
class RandomDataTableTransaction;

/**
 * A RandomDataTableTransaction class provides a simple interface to simulate a transaction running in the system.
 */
class RandomDataTableTransaction {
 public:
  /**
   * Initializes a new RandomDataTableTransaction to work on the given test object
   * @param test_object the test object that runs this transaction
   */
  explicit RandomDataTableTransaction(LargeDataTableBenchmarkObject *test_object);

  /**
   * Destructs a random workload transaction
   */
  ~RandomDataTableTransaction();

  /**
   * Randomly updates a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomUpdate(Random *generator);

  /**
   * Randomly inserts a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomInsert(Random *generator);

  /**
   * Randomly selects a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomSelect(Random *generator);

  /**
   * Finish the simulation of this transaction. The underlying transaction will either commit or abort.
   */
  void Finish();

  transaction::timestamp_t BeginTimestamp() const { return start_time_; }

  transaction::timestamp_t CommitTimestamp() const {
    if (aborted_) return transaction::timestamp_t(static_cast<uint64_t>(-1));
    return commit_time_;
  }

 private:
  friend class LargeDataTableBenchmarkObject;
  LargeDataTableBenchmarkObject *test_object_;
  transaction::TransactionContext *txn_;
  // extra bookkeeping for correctness checks
  bool aborted_;
  transaction::timestamp_t start_time_, commit_time_;
  byte *buffer_;
};

/**
 * A LargeTransactionTest bootstraps a table, and runs randomly generated workloads concurrently against the table to
 * simulate a real run of the system. This works with or without gc.
 *
 * So far we only do updates and selects, as inserts and deletes are not given much special meaning without the index.
 */
class LargeDataTableBenchmarkObject {
 public:
  /**
   * Initializes a test object with the given configuration
   * @param max_columns the max number of columns in the generated test table
   * @param initial_table_size number of tuples the table should have
   * @param txn_length length of every simulated transaction, in number of operations (select or update)
   * @param update_select_ratio the ratio of inserts vs. updates vs. select in the generated transaction
   *                             (e.g. {0.0, 0.3, 0.7} will be 0% inserts, 30% updates, and 70% reads)
   * @param block_store the block store to use for the underlying data table
   * @param buffer_pool the buffer pool to use for simulated transactions
   * @param generator the random generator to use for the test
   * @param gc_on whether gc is enabled
   * @param log_manager pointer to the LogManager if enabled
   */
  LargeDataTableBenchmarkObject(const std::vector<uint16_t> &attr_sizes, uint32_t initial_table_size,
                                uint32_t txn_length, std::vector<double> operation_ratio,
                                storage::BlockStore *block_store, storage::RecordBufferSegmentPool *buffer_pool,
                                std::default_random_engine *generator, bool gc_on,
                                storage::LogManager *log_manager = DISABLED);

  /**
   * Destructs a LargeDataTableBenchmarkObject
   */
  ~LargeDataTableBenchmarkObject();

  /**
   * @return the timestamp manager used by this test
   */
  transaction::TimestampManager *GetTimestampManager() { return &timestamp_manager_; }

  /**
   * @return the transaction manager used by this test
   */
  transaction::TransactionManager *GetTxnManager() { return &txn_manager_; }

  /**
   * Simulate an oltp workload, running the specified number of total transactions while allowing the specified number
   * of transactions to run concurrently. Transactions are generated using the configuration provided on construction.
   *
   * @param num_transactions total number of transactions to run
   * @param num_concurrent_txns number of transactions allowed to run concurrently
   * @param submit_interval_us the interval between submitting two transactions for each thread, including the execution
   * @return abort count, elapsed_ms
   */
  std::pair<uint64_t, uint64_t> SimulateOltp(uint32_t num_transactions, uint32_t num_concurrent_txns,
                                             metrics::MetricsManager *metrics_manager = DISABLED,
                                             uint32_t submit_interval_us = 0);

  /**
   * @return layout of the randomly generated table
   */
  const storage::BlockLayout &Layout() const { return layout_; }

 private:
  void SimulateOneTransaction(RandomDataTableTransaction *txn, uint32_t txn_id);

  template <class Random>
  void PopulateInitialTable(uint32_t num_tuples, Random *generator);

  friend class RandomDataTableTransaction;
  storage::BlockLayout layout_;
  storage::DataTable table_;
  std::default_random_engine *generator_;
  transaction::TransactionContext *initial_txn_;
  uint64_t abort_count_;
  std::vector<double> operation_ratio_;

  // tuple content is meaningless if bookkeeping is off.
  std::vector<storage::TupleSlot> inserted_tuples_;
  transaction::TransactionManager txn_manager_;

  // so we don't have to calculate these over and over again
  storage::ProjectedRowInitializer row_initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));

  transaction::TimestampManager timestamp_manager_;
  uint32_t txn_length_;
  bool gc_on_;
};
}  // namespace terrier
