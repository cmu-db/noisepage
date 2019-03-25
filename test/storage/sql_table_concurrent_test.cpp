#include <algorithm>
#include <cstring>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "storage/sql_table.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"
namespace terrier {
struct SqlTableConcurrentTests : public TerrierTest {
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }

  storage::RecordBufferSegmentPool buffer_pool_{10000, 10000};
  transaction::TransactionManager txn_manager_ = {&buffer_pool_, true, LOGGING_DISABLED};

  storage::BlockStore block_store_{100, 100};
  std::default_random_engine generator_;

  // TODO(yangjuns): need to fake a catalog that maps sql_table -> version_num
};

// NOLINTNEXTLINE
TEST_F(SqlTableConcurrentTests, ConcurrentInsertsWithDifferentVersions) {
  const uint32_t num_iterations = 5;
  const uint32_t num_changes = 1000;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LOG_INFO("iteration {}", iteration);
    catalog::Schema schema = CatalogTestUtil::RandomSchemaNoVarchar(max_columns, &generator_);
    storage::SqlTable test(&block_store_, schema, catalog::table_oid_t(12345));

    { // Begin concurrent section
      auto workload = [&](uint32_t id) {
        transaction::TransactionContext txn = txn_manager_.BeginTransaction();
        // TODO: test workload logic here
        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
        delete txn;
      };

      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    } // End concurrent section
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableConcurrentTests, ConcurrentSelectsWithDifferentVersions) {
  const uint32_t num_iterations = 5;
  const uint32_t num_changes = 1000;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LOG_INFO("iteration {}", iteration);
    catalog::Schema schema = CatalogTestUtil::RandomSchemaNoVarchar(max_columns, &generator_);
    storage::SqlTable test(&block_store_, schema, catalog::table_oid_t(12345));

    { // Begin concurrent section
      auto workload = [&](uint32_t id) {
        transaction::TransactionContext txn = txn_manager_.BeginTransaction();
        // TODO: test workload logic here
        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
        delete txn;
      };

      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    }// End concurrent section
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableConcurrentTests, ConcurrentQueriesWithSchemaChange) {
  const uint32_t num_iterations = 5;
  const uint32_t num_changes = 1000;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LOG_INFO("iteration {}", iteration);
    catalog::Schema schema = CatalogTestUtil::RandomSchemaNoVarchar(max_columns, &generator_);
    storage::SqlTable test(&block_store_, schema, catalog::table_oid_t(12345));

    { // Begin concurrent section
      auto workload = [&](uint32_t id) {
        transaction::TransactionContext txn = txn_manager_.BeginTransaction();
        // TODO: test workload logic here
        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
        delete txn;
      };

      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    }// End concurrent section
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableConcurrentTests, ConcurrentCrossVersionUpdates) {
  const uint32_t num_iterations = 5;
  const uint32_t num_changes = 1000;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LOG_INFO("iteration {}", iteration);
    catalog::Schema schema = CatalogTestUtil::RandomSchemaNoVarchar(max_columns, &generator_);
    storage::SqlTable test(&block_store_, schema, catalog::table_oid_t(12345));

    { // Begin concurrent section
      auto workload = [&](uint32_t id) {
        transaction::TransactionContext txn = txn_manager_.BeginTransaction();
        // TODO: test workload logic here
        txn_manager_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);
        delete txn;
      };

      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    }// End concurrent section
  }
}
}  // namespace terrier
