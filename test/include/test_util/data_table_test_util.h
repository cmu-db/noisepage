#pragma once
#include <algorithm>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "storage/data_table.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace noisepage {

class LargeDataTableTestObject;
class RandomDataTableTransaction;
using TupleEntry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;
using TableSnapshot = std::unordered_map<storage::TupleSlot, storage::ProjectedRow *>;
using VersionedSnapshots = std::map<transaction::timestamp_t, TableSnapshot>;
// {committed, aborted}
using SimulationResult =
    std::pair<std::vector<RandomDataTableTransaction *>, std::vector<RandomDataTableTransaction *>>;

/**
 * Value object that holds various parameters to the random testing framework.
 * Not every member is required for every test, and it is okay to leave some of them
 * out when constructing if none is required.
 */
class LargeDataTableTestConfiguration {
 public:
  /**
   * Helper class to build a new test configuration
   */
  class Builder {
   public:
    /**
     * @param num_iterations number of independent runs of the test to perform
     * @return self-reference
     */
    Builder &SetNumIterations(uint32_t num_iterations) {
      num_iterations_ = num_iterations;
      return *this;
    }

    /**
     * @param num_txns number of transaction to run in total
     * @return self-reference
     */
    Builder &SetNumTxns(uint32_t num_txns) {
      num_txns_ = num_txns;
      return *this;
    }

    /**
     * @param batch_size (GC tests only) The interval (num transactions) at which to perform a check
     * @return self-reference
     */
    Builder &SetBatchSize(uint32_t batch_size) {
      batch_size_ = batch_size;
      return *this;
    }

    /**
     * @param num_concurrent_txns number of concurrent workers to run transactions on
     * @return self-reference
     */
    Builder &SetNumConcurrentTxns(uint32_t num_concurrent_txns) {
      num_concurrent_txns_ = num_concurrent_txns;
      return *this;
    }

    /**
     * @param num_iterations the ratio of updates vs. select in the generated transaction
     *                             (e.g. {0.3, 0.7} will be 30% updates and 70% reads)
     * @return self-reference
     */
    Builder &SetUpdateSelectRatio(std::vector<double> update_select_ratio) {
      update_select_ratio_ = std::move(update_select_ratio);
      return *this;
    }

    /**
     * @param txn_length length of every simulated transaction, in number of operations (select or update)
     * @return self-reference
     */
    Builder &SetTxnLength(uint32_t txn_length) {
      txn_length_ = txn_length;
      return *this;
    }

    /**
     * @param initial_table_size number of tuples the table should have
     * @return self-reference
     */
    Builder &SetInitialTableSize(uint32_t initial_table_size) {
      initial_table_size_ = initial_table_size;
      return *this;
    }

    /**
     * @param max_columns the max number of columns in the generated test table
     * @return self-reference
     */
    Builder &SetMaxColumns(uint16_t max_columns) {
      max_columns_ = max_columns;
      return *this;
    }

    /**
     * @param allowed whether varlen columns are allowed in the generated test table
     * @return self-reference
     */
    Builder &SetVarlenAllowed(bool allowed) {
      varlen_allowed_ = allowed;
      return *this;
    }

    /**
     * @return the constructed LargeDataTableTestConfiguration object
     */
    LargeDataTableTestConfiguration Build() {
      return {num_iterations_, num_txns_,           batch_size_,  num_concurrent_txns_, std::move(update_select_ratio_),
              txn_length_,     initial_table_size_, max_columns_, varlen_allowed_};
    }

   private:
    uint32_t num_iterations_ = 0;
    uint32_t num_txns_ = 0;
    uint32_t batch_size_ = 0;
    uint32_t num_concurrent_txns_ = 0;
    std::vector<double> update_select_ratio_;
    uint32_t txn_length_ = 0;
    uint32_t initial_table_size_ = 0;
    uint16_t max_columns_ = 0;
    bool varlen_allowed_ = false;
  };

  uint32_t NumIterations() const { return num_iterations_; }
  uint32_t NumTxns() const { return num_txns_; }
  uint32_t BatchSize() const { return batch_size_; }
  uint32_t NumConcurrentTxns() const { return num_concurrent_txns_; }
  const std::vector<double> &UpdateSelectRatio() const { return update_select_ratio_; }
  uint32_t TxnLength() const { return txn_length_; }
  uint32_t InitialTableSize() const { return initial_table_size_; }
  uint16_t MaxColumns() const { return max_columns_; }
  bool VarlenAllowed() const { return varlen_allowed_; }

  static LargeDataTableTestConfiguration Empty() { return Builder().Build(); }

 private:
  LargeDataTableTestConfiguration(const uint32_t num_iterations, const uint32_t num_txns, const uint32_t batch_size,
                                  const uint32_t num_concurrent_txns, std::vector<double> update_select_ratio,
                                  const uint32_t txn_length, const uint32_t initial_table_size,
                                  const uint16_t max_columns, bool varlen_allowed)
      : num_iterations_(num_iterations),
        num_txns_(num_txns),
        batch_size_(batch_size),
        num_concurrent_txns_(num_concurrent_txns),
        update_select_ratio_(std::move(update_select_ratio)),
        txn_length_(txn_length),
        initial_table_size_(initial_table_size),
        max_columns_(max_columns),
        varlen_allowed_(varlen_allowed) {}

 private:
  const uint32_t num_iterations_;
  const uint32_t num_txns_;
  const uint32_t batch_size_;
  const uint32_t num_concurrent_txns_;
  std::vector<double> update_select_ratio_;
  const uint32_t txn_length_;
  const uint32_t initial_table_size_;
  const uint16_t max_columns_;
  bool varlen_allowed_;
};

/**
 * A RandomDataTableTransaction class provides a simple interface to simulate a transaction running in the system.
 *
 * The transaction can be initialized to store enough information to allow for correctness checking, or take care
 * of transaction recycling when GC is not turned on. Disable correctness record-keeping for test cases where you
 * don't care about correctness.
 */
// TODO(Tianyu): We do not support inserts and deletes interleaved in this randomized test yet. Given our storage model,
// those operations are not really different from updates except the execution layer will interpret tuples differently,
// and GC needs to recycle slots. Maybe we can write those later, but I suspect the gain will be minimal compared to
// the extra effort.
class RandomDataTableTransaction {
 public:
  /**
   * Initializes a new RandomDataTableTransaction to work on the given test object
   * @param test_object the test object that runs this transaction
   */
  explicit RandomDataTableTransaction(LargeDataTableTestObject *test_object);

  /**
   * Destructs a random workload transaction
   */
  ~RandomDataTableTransaction();

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

  transaction::timestamp_t BeginTimestamp() const { return start_time_; }

  transaction::timestamp_t CommitTimestamp() const {
    if (aborted_) return transaction::timestamp_t(static_cast<uint64_t>(-1));
    return commit_time_;
  }

  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> *Updates() { return &updates_; }

 private:
  friend class LargeDataTableTestObject;
  LargeDataTableTestObject *test_object_;
  transaction::TransactionContext *txn_;
  // extra bookkeeping for correctness checks
  bool aborted_;
  transaction::timestamp_t start_time_, commit_time_;
  std::vector<TupleEntry> selects_;
  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> updates_;
};

/**
 * A LargeTransactionTest bootstraps a table, and runs randomly generated workloads concurrently against the table to
 * simulate a real run of the system. This works with or without gc, and comes with optional correctness checks.
 * Correctness checks add significant memory and performance overhead.
 *
 * So far we only do updates and selects, as inserts and deletes are not given much special meaning without the index.
 */
class LargeDataTableTestObject {
 public:
  /**
   * Initializes a test object with the given configuration
   * @param config test configuration object
   * @param block_store block store to use
   * @param txn_manager transaction manager to use
   * @param generator source of randomness
   * TODO(Tianyu): This is currently only used to see if the system enables logging. If we expose that information
   *               in transaction manager, presumably we don't need to take in a log manager anymore
   * @param log_manager log manager to use
   */
  LargeDataTableTestObject(const LargeDataTableTestConfiguration &config, storage::BlockStore *block_store,
                           transaction::TransactionManager *txn_manager, std::default_random_engine *generator,
                           storage::LogManager *log_manager);
  /**
   * Destructs a LargeDataTableTestObject
   */
  ~LargeDataTableTestObject();

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
   * @return layout of the randomly generated table
   */
  const storage::BlockLayout &Layout() const { return layout_; }

  /**
   * Checks the correctness of reads in the committed transactions. No committed transaction should have read some
   * version of the tuple outside of its version. The correct version is reconstructed using the last valid image of
   * the table, either initial or the newest version last time this method is called, and the list of updates committed.
   * @param commits list of commits to check.
   */
  // TODO(Tianyu): Interesting thought: If we let an external correctness checker share the list of
  // RandomDataTableTransaction objects, we can in theory check correctness as more operations are run, and
  // keep the memory consumption of all this bookkeeping down. (Just like checkpoints)
  void CheckReadsCorrect(std::vector<RandomDataTableTransaction *> *commits);

  void SimulateOneTransaction(RandomDataTableTransaction *txn, uint32_t txn_id);

  template <class Random>
  void PopulateInitialTable(uint32_t num_tuples, Random *generator);

  storage::ProjectedRow *CopyTuple(storage::ProjectedRow *other);

  void UpdateSnapshot(RandomDataTableTransaction *txn, TableSnapshot *curr, const TableSnapshot &before);

  // This returned value will contain memory that has to be freed manually
  VersionedSnapshots ReconstructVersionedTable(std::vector<RandomDataTableTransaction *> *txns);

  void CheckTransactionReadCorrect(RandomDataTableTransaction *txn, const VersionedSnapshots &snapshots);

  void UpdateLastCheckedVersion(const TableSnapshot &snapshot);

  friend class RandomDataTableTransaction;
  uint32_t txn_length_;
  std::vector<double> update_select_ratio_;
  std::default_random_engine *generator_;
  storage::BlockLayout layout_;
  storage::DataTable table_;
  transaction::TransactionManager *txn_manager_;
  transaction::TransactionContext *initial_txn_ = nullptr;
  bool gc_on_, wal_on_;

  // tuple content is meaningless if bookkeeping is off.
  std::vector<TupleEntry> last_checked_version_;
  // so we don't have to calculate these over and over again
  storage::ProjectedRowInitializer row_initializer_ =
      storage::ProjectedRowInitializer::Create(layout_, StorageTestUtil::ProjectionListAllColumns(layout_));
};
}  // namespace noisepage
