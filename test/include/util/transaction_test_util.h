#pragma once
#include <algorithm>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/container/concurrent_vector.h"
#include "gtest/gtest.h"
#include "storage/data_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"
#include "util/storage_test_util.h"
#include "util/test_harness.h"

namespace terrier {
class LargeTransactionTestObject;
class RandomWorkloadTransaction;
using TupleEntry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;
using TableSnapshot = std::unordered_map<storage::TupleSlot, storage::ProjectedRow *>;
using VersionedSnapshots = std::map<timestamp_t, TableSnapshot>;
// {committed, aborted}
using SimulationResult = std::pair<std::vector<RandomWorkloadTransaction *>, std::vector<RandomWorkloadTransaction *>>;

/**
 * A RandomWorkloadTransaction class provides a simple interface to simulate a transaction running in the system.
 *
 * The transaction can be initialized to store enough information to allow for correctness checking, or take care
 * of transaction recycling when GC is not turned on. Disable correctness record-keeping for test cases where you
 * don't care about correctness.
 */
// TODO(Tianyu): We do not support inserts and deletes interleaved in this randomized test yet. Given our storage model,
// those operations are not really different from updates except the execution layer will interpret tuples differently,
// and GC needs to recycle slots. Maybe we can write those later, but I suspect the gain will be minimal compared to
// the extra effort.
class RandomWorkloadTransaction {
 public:
  /**
   * Initializes a new RandomWorkloadTransaction to work on the given test object
   * @param test_object the test object that runs this transaction
   */
  explicit RandomWorkloadTransaction(LargeTransactionTestObject *test_object);

  /**
   * Destructs a random workload transaction
   */
  ~RandomWorkloadTransaction();

  /**
   * Randomly updates a tuple, using the given generator as source of randomness. Operation is logged if correctness
   * check is turned on in the calling test object.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomUpdate(Random *generator);

  /**
   * Randomly selects a tuple, using the given generator as source of randomness. Operation is logged if correctness
   * check is turned on in the calling test object.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomSelect(Random *generator);

  /**
   * Finish the simulation of this transaction. The underlying transaction will either commit or abort, and necessary
   * logs are taken if correctness check is turned on.
   */
  void Finish();

 private:
  friend class LargeTransactionTestObject;
  LargeTransactionTestObject *test_object_;
  transaction::TransactionContext *txn_;
  // extra bookkeeping for correctness checks
  bool aborted_;
  timestamp_t start_time_, commit_time_;
  std::vector<TupleEntry> selects_;
  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> updates_;
  byte *buffer_;
};

/**
 * A LargeTransactionTest bootstraps a table, and runs randomly generated workloads concurrently against the table to
 * simulate a real run of the system. This works with or without gc, and comes with optional correctness checks.
 * Correctness checks add significant memory and performance overhead.
 *
 * So far we only do updates and selects, as inserts and deletes are not given much special meaning without the index.
 */
class LargeTransactionTestObject {
 public:
  /**
   * Initializes a test object with the given configuration
   * @param max_columns the max number of columns in the generated test table
   * @param initial_table_size number of tuples the table should have
   * @param txn_length length of every simulated transaction, in number of operations (select or update)
   * @param update_select_ratio the ratio of updates vs. select in the generated transaction
   *                             (e.g. {0.3, 0.7} will be 30% updates and 70% reads)
   * @param block_store the block store to use for the underlying data table
   * @param buffer_pool the buffer pool to use for simulated transactions
   * @param generator the random generator to use for the test
   * @param gc_on whether gc is enabled
   * @param bookkeeping whether correctness check is enabled
   */
  LargeTransactionTestObject(uint16_t max_columns, uint32_t initial_table_size, uint32_t txn_length,
                             std::vector<double> update_select_ratio, storage::BlockStore *block_store,
                             common::ObjectPool<storage::BufferSegment> *buffer_pool,
                             std::default_random_engine *generator, bool gc_on, bool bookkeeping);

  /**
   * Destructs a LargeTransactionTestObject
   */
  ~LargeTransactionTestObject();

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
   * @return a list of transaction logs if correctness checks are enabled, that can be checked for consistency. (these
   * will need to be freed manually.), or empty otherwise.
   */
  SimulationResult SimulateOltp(uint32_t num_transactions, uint32_t num_concurrent_txns);

  /**
   * Checks the correctness of reads in the committed transactions. No committed transaction should have read some
   * version of the tuple outside of its version. The correct version is reconstructed using the last valid image of
   * the table, either initial or the newest version last time this method is called, and the list of updates committed.
   * @param commits list of commits to check.
   */
  // TODO(Tianyu): Interesting thought: If we let an external correctness checker share the list of
  // RandomWorkloadTransaction objects, we can in theory check correctness as more operations are run, and
  // keep the memory consumption of all this bookkeeping down. (Just like checkpoints)
  void CheckReadsCorrect(std::vector<RandomWorkloadTransaction *> *commits);

 private:
  void SimulateOneTransaction(RandomWorkloadTransaction *txn, uint32_t txn_id);

  template <class Random>
  void PopulateInitialTable(uint32_t num_tuples, Random *generator);

  storage::ProjectedRow *CopyTuple(storage::ProjectedRow *other);

  void UpdateSnapshot(RandomWorkloadTransaction *txn, TableSnapshot *curr, const TableSnapshot &before);

  // This returned value will contain memory that has to be freed manually
  VersionedSnapshots ReconstructVersionedTable(std::vector<RandomWorkloadTransaction *> *txns);

  void CheckTransactionReadCorrect(RandomWorkloadTransaction *txn, const VersionedSnapshots &snapshots);

  void UpdateLastCheckedVersion(const TableSnapshot &snapshot);

  friend class RandomWorkloadTransaction;
  uint32_t txn_length_;
  std::vector<double> update_select_ratio_;
  std::default_random_engine *generator_;
  storage::BlockLayout layout_;
  storage::DataTable table_;
  transaction::TransactionManager txn_manager_;
  transaction::TransactionContext *initial_txn_;
  bool gc_on_, bookkeeping_;
  // tuple content is meaningless if bookkeeping is off.
  std::vector<TupleEntry> last_checked_version_;

  // so we don't have to calculate these over and over again
  storage::ProjectedRowInitializer row_initializer_{layout_, StorageTestUtil::ProjectionListAllColumns(layout_)};
};
}  // namespace terrier
