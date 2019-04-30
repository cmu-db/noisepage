#include <vector>
#include "storage/garbage_collector.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier {

class DeferredActionsTest : public TerrierTest {
 protected:
  DeferredActionsTest() : gc_(&txn_mgr_) {}

  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override {
    gc_.PerformGarbageCollection();
    gc_.PerformGarbageCollection();
    TerrierTest::TearDown();
  }

  storage::RecordBufferSegmentPool buffer_pool_ = {100, 100};
  transaction::TransactionManager txn_mgr_ = {&buffer_pool_, true, LOGGING_DISABLED};
  storage::GarbageCollector gc_;
};

// Test that abort actions do not execute before the transaction aborts and that
// commit actions are never executed.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, AbortAction) {
  auto *txn = txn_mgr_.BeginTransaction();

  bool aborted = false;
  bool committed = false;
  txn->RegisterAbortAction([&]() { aborted = true; });
  txn->RegisterCommitAction([&]() { committed = true; });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);

  txn_mgr_.Abort(txn);

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
}

// Test that commit actions are not executed before the transaction commits and
// that abort actions are never executed.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, CommitAction) {
  // Setup an entire database so that we can do a updating commit
  auto col_oid = catalog::col_oid_t(42);
  std::vector<catalog::col_oid_t> col_oids;
  col_oids.emplace_back(col_oid);

  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("dummy", type::TypeId::INTEGER, false, col_oid);

  storage::BlockStore block_store{100, 100};
  catalog::Schema schema(cols);
  storage::SqlTable table(&block_store, schema, catalog::table_oid_t(24));

  auto row_pair = table.InitializerForProjectedRow(col_oids);
  auto pri = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
  auto pr_map = new storage::ProjectionMap(std::get<1>(row_pair));

  auto insert_buffer = common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
  auto insert = pri->InitializeRow(insert_buffer);

  auto *txn = txn_mgr_.BeginTransaction();

  bool aborted = false;
  bool committed = false;
  txn->RegisterAbortAction([&]() { aborted = true; });
  txn->RegisterCommitAction([&]() { committed = true; });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);

  auto *data = reinterpret_cast<int32_t *>(insert->AccessForceNotNull(pr_map->at(col_oid)));
  *data = 42;
  table.Insert(txn, *insert);

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);

  txn_mgr_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);

  EXPECT_TRUE(committed);
  EXPECT_FALSE(aborted);

  gc_.PerformGarbageCollection();
  gc_.PerformGarbageCollection();

  insert = nullptr;
  delete[] insert_buffer;
  delete pr_map;
  delete pri;
}

// Test that the GC performs available deferred actions when PerformGarbageCollection is called
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, SimpleDefer) {
  bool deferred = false;
  txn_mgr_.DeferAction([&]() { deferred = true; });

  EXPECT_FALSE(deferred);

  gc_.PerformGarbageCollection();

  EXPECT_TRUE(deferred);
}

// Test that the GC correctly delays execution of deferred actions until the
// epoch (oldest running transaction) is greater than or equal to the next
// available timestamp when the action was deferred.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, DelayedDefer) {
  auto *txn = txn_mgr_.BeginTransaction();

  bool deferred = false;
  txn_mgr_.DeferAction([&]() { deferred = true; });

  EXPECT_FALSE(deferred);

  gc_.PerformGarbageCollection();

  EXPECT_FALSE(deferred);  // txn is still open

  txn_mgr_.Abort(txn);

  gc_.PerformGarbageCollection();

  EXPECT_TRUE(deferred);
}

// Test that a deferred action can successfully generate and insert another
// deferred action (e.g. an "unlink" action could generate the paired "delete")
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, ChainedDefer) {
  bool defer1 = false;
  bool defer2 = false;

  txn_mgr_.DeferAction([&]() {
    defer1 = true;
    txn_mgr_.DeferAction([&]() { defer2 = true; });
  });

  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_.PerformGarbageCollection();

  EXPECT_TRUE(defer1);
  EXPECT_FALSE(defer2);  // Sitting in txn_mgr_'s deferral queue

  gc_.PerformGarbageCollection();

  EXPECT_TRUE(defer1);
  EXPECT_TRUE(defer2);
}

// Test that the transaction context's interface supports creating a deep deferral
// chain that conditionally executes only on abort.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, AbortBootstrapDefer) {
  auto *txn = txn_mgr_.BeginTransaction();

  bool defer1 = false;
  bool defer2 = false;
  bool aborted = false;
  bool committed = false;

  txn->RegisterCommitAction([&]() { committed = true; });

  // Bootstrap into the lamda.  Need to eliminate the reference to txn since
  // it could get garbage collected before the lambda derefernces it.
  auto tm = txn->GetTransactionManager();
  txn->RegisterAbortAction([&, tm]() {
    aborted = true;
    tm->DeferAction([&, tm]() {
      defer1 = true;
      tm->DeferAction([&]() { defer2 = true; });
    });
  });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_.PerformGarbageCollection();

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  txn_mgr_.Abort(txn);

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_.PerformGarbageCollection();

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_FALSE(defer2);

  gc_.PerformGarbageCollection();

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_TRUE(defer2);
}

// Test that the transaction context's interface supports creating a deep deferral
// chain that conditionally executes only on commit.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, CommitBootstrapDefer) {
  auto col_oid = catalog::col_oid_t(42);
  std::vector<catalog::col_oid_t> col_oids;
  col_oids.emplace_back(col_oid);

  std::vector<catalog::Schema::Column> cols;
  cols.emplace_back("dummy", type::TypeId::INTEGER, false, col_oid);

  storage::BlockStore block_store{100, 100};
  catalog::Schema schema(cols);
  storage::SqlTable table(&block_store, schema, catalog::table_oid_t(24));

  auto row_pair = table.InitializerForProjectedRow(col_oids);
  auto pri = new storage::ProjectedRowInitializer(std::get<0>(row_pair));
  auto pr_map = new storage::ProjectionMap(std::get<1>(row_pair));

  auto insert_buffer = common::AllocationUtil::AllocateAligned(pri->ProjectedRowSize());
  auto insert = pri->InitializeRow(insert_buffer);

  auto *txn = txn_mgr_.BeginTransaction();

  bool defer1 = false;
  bool defer2 = false;
  bool aborted = false;
  bool committed = false;

  txn->RegisterAbortAction([&]() { aborted = true; });

  // Bootstrap into the lamda.  Need to eliminate the reference to txn since
  // it could get garbage collected before the lambda derefernces it.
  auto tm = txn->GetTransactionManager();
  txn->RegisterCommitAction([&, tm]() {
    committed = true;
    tm->DeferAction([&, tm]() {
      defer1 = true;
      tm->DeferAction([&]() { defer2 = true; });
    });
  });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  auto *data = reinterpret_cast<int32_t *>(insert->AccessForceNotNull(pr_map->at(col_oid)));
  *data = 42;
  table.Insert(txn, *insert);

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_.PerformGarbageCollection();

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  txn_mgr_.Commit(txn, TestCallbacks::EmptyCallback, nullptr);

  EXPECT_FALSE(aborted);
  EXPECT_TRUE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_.PerformGarbageCollection();

  EXPECT_FALSE(aborted);
  EXPECT_TRUE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_FALSE(defer2);

  gc_.PerformGarbageCollection();

  EXPECT_FALSE(aborted);
  EXPECT_TRUE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_TRUE(defer2);

  insert = nullptr;
  delete[] insert_buffer;
  delete pr_map;
  delete pri;
}
}  // namespace terrier
