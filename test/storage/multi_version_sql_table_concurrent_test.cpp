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
TEST_F(SqlTableConcurrentTests, ConcurrentInsert) {
  const uint32_t num_iterations = 5;
  const uint32_t num_inserts = 10000;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LOG_INFO("iteration {}", iteration);
    catalog::Schema schema = CatalogTestUtil::RandomSchemaNoVarchar(max_columns, &generator_);
    storage::SqlTable test(&block_store_, schema, catalog::table_oid_t(12345));
    std::vector<transaction::TransactionContext *> txns;
    std::vector<std::unordered_map<storage::TupleSlot, storage::ProjectedRow *>> reference(num_threads);
    for (uint32_t thread = 0; thread < num_threads; thread++) {
      txns.emplace_back(txn_manager_.BeginTransaction());
    }

    // get version number
    storage::layout_version_t version(0);

    // get all columns
    std::vector<catalog::col_oid_t> all_col_oids;
    for (auto &col : schema.GetColumns()) all_col_oids.emplace_back(col.GetOid());

    // generate workload
    auto workload = [&](uint32_t id) {
      // Insert Random Tuples
      for (uint32_t i = 0; i < num_inserts / num_threads; i++) {
        storage::ProjectedRow *pr = CatalogTestUtil::RandomInsertRow(&test, schema, version, &generator_);
        // Insert tuples
        storage::TupleSlot slot = test.Insert(txns[id], *pr, version);
        reference[id][slot] = pr;
      }
      EXPECT_EQ(reference[id].size(), num_inserts / num_threads);
      // Read tuples
      auto pr_pair = test.InitializerForProjectedRow(all_col_oids, version);
      auto *select_buffer = common::AllocationUtil::AllocateAligned(pr_pair.first.ProjectedRowSize());
      storage::ProjectedRow *pr = pr_pair.first.InitializeRow(select_buffer);
      EXPECT_NE(pr, nullptr);
      for (auto &it : reference[id]) {
        EXPECT_TRUE(test.Select(txns[id], it.first, pr, pr_pair.second, version));
        EXPECT_TRUE(CatalogTestUtil::ProjectionListEqual(schema, pr, it.second, pr_pair.second));
      }
      delete[] select_buffer;
      txn_manager_.Commit(txns[id], TestCallbacks::EmptyCallback, nullptr);
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    // clean up memoery
    for (auto txn : txns) delete txn;
    for (auto &map : reference) {
      for (auto &p : map) {
        delete[] reinterpret_cast<byte *>(p.second);
      }
    }
  }
}

// NOLINTNEXTLINE
TEST_F(SqlTableConcurrentTests, ConcurrentChangeSchema) {
  const uint32_t num_iterations = 5;
  const uint32_t num_changes = 1000;
  const uint16_t max_columns = 20;
  const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iteration = 0; iteration < num_iterations; iteration++) {
    LOG_INFO("iteration {}", iteration);
    catalog::Schema schema = CatalogTestUtil::RandomSchemaNoVarchar(max_columns, &generator_);
    storage::SqlTable test(&block_store_, schema, catalog::table_oid_t(12345));
    std::vector<transaction::TransactionContext *> txns;

    for (uint32_t thread = 0; thread < num_threads; thread++) {
      txns.emplace_back(txn_manager_.BeginTransaction());
    }

    // generate workload
    auto workload = [&](uint32_t id) {
      // Get random schema
      for (uint32_t i = 0; i < num_changes / num_threads; i++) {
        catalog::Schema schema = CatalogTestUtil::RandomSchemaNoVarchar(max_columns, &generator_);
        test.ChangeSchema(txns[id], schema);
      }
      txn_manager_.Commit(txns[id], TestCallbacks::EmptyCallback, nullptr);
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);

    // clean up memoery
    for (auto txn : txns) delete txn;
  }
}
}  // namespace terrier
