#pragma once
#include <algorithm>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "storage/garbage_collector.h"
#include "test_util/storage_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace terrier {

class LargeSqlTableTestObject;
class RandomSqlTableTransaction;
using TupleEntry = std::pair<storage::TupleSlot, storage::ProjectedRow *>;
using TableSnapshot = std::unordered_map<storage::TupleSlot, storage::ProjectedRow *>;
using VersionedSnapshots = std::map<transaction::timestamp_t, TableSnapshot>;

/**
 * Defines the test configurations to be used for a LargeSqlTableTestObject
 */
class LargeSqlTableTestConfiguration {
 public:
  /**
   * Builder class for LargeSqlTableTestConfiguration
   */
  class Builder {
   public:
    /**
     * @param num_databases the number of databases in the generated test object
     * @return self-reference for method chaining
     */
    Builder &SetNumDatabases(uint16_t num_databases) {
      builder_num_databases_ = num_databases;
      return *this;
    }

    /**
     * @param num_tables the number of tables PER DATABASE in the generated test object
     * @return self-reference for method chaining
     */
    Builder &SetNumTables(uint16_t num_tables) {
      builder_num_tables_ = num_tables;
      return *this;
    }

    /**
     * @param max_columns the max number of columns in the generated test tables
     * @return self-reference for method chaining
     */
    Builder &SetMaxColumns(uint16_t max_columns) {
      builder_max_columns_ = max_columns;
      return *this;
    }

    /**
     * @param initial_table_size number of tuples each table should have initially
     * @return self-reference for method chaining
     */
    Builder &SetInitialTableSize(uint32_t initial_table_size) {
      builder_initial_table_size_ = initial_table_size;
      return *this;
    }

    /**
     * @param txn_length length of every simulated transaction, in number of operations (select or update)
     * @return self-reference for method chaining
     */
    Builder &SetTxnLength(uint32_t txn_length) {
      builder_txn_length_ = txn_length;
      return *this;
    }

    /**
     * @param insert_update_select_delete_ratio the ratio of inserts vs. updates vs. select vs. deletes in the generated
     * transaction (e.g. {0.1, 0.2, 0.6. 0.1} will be 10% inserts, 20% updates, 60% reads, and 10% deletes)
     * @warning the number of deletes should not exceed the number of tuples inserted initially
     * @return self-reference for method chaining
     */
    Builder &SetInsertUpdateSelectDeleteRatio(std::vector<double> ratios) {
      TERRIER_ASSERT(ratios.size() == 4, "Ratio must have four values");
      builder_insert_update_select_delete_ratio_ = std::move(ratios);
      return *this;
    }

    /**
     * @param varlen_allowed if we allow varlen columns to show up in the block layout
     * @return self-reference for method chaining
     */
    Builder &SetVarlenAllowed(bool varlen_allowed) {
      builder_varlen_allowed_ = varlen_allowed;
      return *this;
    }

    /**
     * @return the constructed LargeSqlTableTestConfiguration using the parameters provided
     * (or default ones if not supplied).
     */
    LargeSqlTableTestConfiguration Build() {
      return {builder_num_databases_,      builder_num_tables_, builder_max_columns_,
              builder_initial_table_size_, builder_txn_length_, builder_insert_update_select_delete_ratio_,
              builder_varlen_allowed_};
    }

   private:
    uint16_t builder_num_databases_ = 1;
    uint16_t builder_num_tables_ = 1;
    uint16_t builder_max_columns_ = 25;
    uint32_t builder_initial_table_size_ = 25;
    uint32_t builder_txn_length_ = 25;
    std::vector<double> builder_insert_update_select_delete_ratio_;
    bool builder_varlen_allowed_ = false;
  };

  /**
   * @param num_databases number of databases to create
   * @param num_tables number of tables per database to create
   * @param max_columns the max number of columns in each generated test table
   * @param initial_table_size number of tuples each table should have
   * @param txn_length length of every simulated transaction, in number of operations (select or update)
   * @param insert_update_select_delete_ratio the ratio of inserts vs. updates vs. select vs. deletes in the generated
   * transaction (e.g. {0.1, 0.2, 0.6. 0.1} will be 10% inserts, 20% updates, 60% reads, and 10% deletes)
   * @param varlen_allowed true if varlen columns are allowed
   */
  LargeSqlTableTestConfiguration(const uint16_t num_databases, const uint16_t num_tables, const uint16_t max_columns,
                                 const uint32_t initial_table_size, const uint32_t txn_length,
                                 std::vector<double> insert_update_select_delete_ratio, const bool varlen_allowed)
      : num_databases_(num_databases),
        num_tables_(num_tables),
        max_columns_(max_columns),
        initial_table_size_(initial_table_size),
        txn_length_(txn_length),
        insert_update_select_delete_ratio_(std::move(insert_update_select_delete_ratio)),
        varlen_allowed_(varlen_allowed) {}

 private:
  friend class LargeSqlTableTestObject;
  uint16_t num_databases_;
  uint16_t num_tables_;
  uint16_t max_columns_;
  uint32_t initial_table_size_;
  uint32_t txn_length_;
  std::vector<double> insert_update_select_delete_ratio_;
  bool varlen_allowed_;
};
/**
 * A RandomSqlTableTransaction class provides a simple interface to simulate a transaction that interfaces with the
 * SqlTable layer running in the system. Does not provide bookkeeping for correctness functionality.
 */
class RandomSqlTableTransaction {
 public:
  /**
   * Initializes a new RandomSqlTableTransaction to work on the given test object
   * @param test_object the test object that runs this transaction
   */
  explicit RandomSqlTableTransaction(LargeSqlTableTestObject *test_object);

  /**
   * Destructs a random workload transaction. Does not delete underlyign txn,
   * garbage collection will clean that up.
   */
  ~RandomSqlTableTransaction() = default;

  /**
   * Inserts a tuple with random data, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomInsert(Random *generator);

  /**
   * Randomly updates a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomUpdate(Random *generator);

  /**
   * Randomly deletes a tuple, using the given generator as source of randomness.
   *
   * @tparam Random the type of random generator to use
   * @param generator the random generator to use
   */
  template <class Random>
  void RandomDelete(Random *generator);

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

 private:
  friend class LargeSqlTableTestObject;
  LargeSqlTableTestObject *test_object_;
  transaction::TransactionContext *txn_;
  // extra bookkeeping for abort count
  bool aborted_;
  // We defer adding our inserted tuples into our test objects metadata until we commit
  std::unordered_map<catalog::db_oid_t, std::unordered_map<catalog::table_oid_t, std::vector<storage::TupleSlot>>>
      inserted_tuples_;
};

// TODO(Gus): Add indexes
/**
 * A LargeSqlTableTestObject can bootstrap multiple databases and tables, and runs randomly generated workloads
 * concurrently against the tables to simulate a real run of the system.
 *
 * The test object supports, updates, selects, and deletes
 */
class LargeSqlTableTestObject {
 private:
  /*
   * Holds meta data for tables created by test object
   */
  struct SqlTableMetadata {
    // Column oids for this table. We cache them to generate random updates. They never change because we don't make ddl
    // changes
    std::vector<catalog::col_oid_t> col_oids_;
    // Tuple slots inserted into this sql table
    std::vector<storage::TupleSlot> inserted_tuples_;
    // Latch to protect inserted tuples to allow for concurrent transactions
    common::SpinLatch inserted_tuples_latch_;
    // Buffer for select queries. Not thread safe, but since we aren't doing bookkeeping, it doesn't matter
    byte *buffer_;
  };

 public:
  /**
   * Initializes a test object with the given configuration
   * @param test configuration object
   * @param txn_manager txn manager to use for test object
   * @param catalog catalog to use for test object
   * @param block_store block store for table creation
   * @param generator the random generator to use for the test
   */
  LargeSqlTableTestObject(const LargeSqlTableTestConfiguration &config, transaction::TransactionManager *txn_manager,
                          catalog::Catalog *catalog, storage::BlockStore *block_store,
                          std::default_random_engine *generator);
  /**
   * Destructs a LargeSqlTableTestObject
   */
  ~LargeSqlTableTestObject();

  /**
   * Simulate an oltp workload, running the specified number of total transactions while allowing the specified number
   * of transactions to run concurrently. Transactions are generated using the configuration provided on construction.
   *
   * @param num_transactions total number of transactions to run
   * @param num_concurrent_txns number of transactions allowed to run concurrently
   * @return number of aborted transactions
   */
  uint64_t SimulateOltp(uint32_t num_transactions, uint32_t num_concurrent_txns);

  /**
   * @return map of databases to tables created by the test object
   */
  const std::unordered_map<catalog::db_oid_t, std::vector<catalog::table_oid_t>> &GetTables() const {
    return table_oids_;
  }

  const std::vector<storage::TupleSlot> &GetTupleSlotsForTable(catalog::db_oid_t db_oid,
                                                               catalog::table_oid_t table_oid) {
    TERRIER_ASSERT(tables_.find(db_oid) != tables_.end(), "Requested database was not created");
    TERRIER_ASSERT(tables_[db_oid].find(table_oid) != tables_[db_oid].end(), "Requested table was not created");
    return tables_[db_oid][table_oid]->inserted_tuples_;
  }

 private:
  void SimulateOneTransaction(RandomSqlTableTransaction *txn, uint32_t txn_id);

  template <class Random>
  void PopulateInitialTables(uint16_t num_databases, uint16_t num_tables, uint16_t max_columns, uint32_t num_tuples,
                             bool varlen_allowed, storage::BlockStore *block_store, Random *generator);

  friend class RandomSqlTableTransaction;
  FRIEND_TEST(RecoveryTests, DoubleRecoveryTest);
  uint32_t txn_length_;
  std::vector<double> insert_update_select_delete_ratio_;
  std::default_random_engine *generator_;
  transaction::TransactionManager *txn_manager_;
  catalog::Catalog *catalog_;
  transaction::TransactionContext *initial_txn_;
  uint64_t abort_count_ = 0;
  // So we can easily get a random database and table oid
  std::vector<catalog::db_oid_t> database_oids_;
  std::unordered_map<catalog::db_oid_t, std::vector<catalog::table_oid_t>> table_oids_;

  // Maps database and table oids to struct holding testing metadata for each table
  std::unordered_map<catalog::db_oid_t, std::unordered_map<catalog::table_oid_t, SqlTableMetadata *>> tables_;
};
}  // namespace terrier
