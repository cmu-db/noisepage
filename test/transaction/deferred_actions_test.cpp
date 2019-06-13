#include <vector>
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"
#include "util/catalog_test_util.h"
#include "util/test_harness.h"
#include "util/transaction_test_util.h"

namespace terrier {

class DeferredActionsTest : public TerrierTest {
 protected:
  void TearDown() override {
    while (deferred_action_manager_.Process() != 0) {
    }
    TerrierTest::TearDown();
  }

  storage::RecordBufferSegmentPool buffer_pool_ = {100, 100};
  transaction::TimestampManager timestamp_manager_;
  transaction::DeferredActionManager deferred_action_manager_{&timestamp_manager_};
  storage::VersionChainGC version_chain_gc_{&deferred_action_manager_};
  transaction::TransactionManager txn_mgr_ = {&timestamp_manager_, &buffer_pool_, &deferred_action_manager_,
                                              &version_chain_gc_, DISABLED};
};

// Test that abort actions do not execute before the transaction aborts and that
// commit actions are never executed.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, AbortAction) {
  auto *txn = txn_mgr_.BeginTransaction();

  bool aborted = false;
  bool committed = false;
  txn->RegisterAbortAction([&](transaction::DeferredActionManager *) { aborted = true; });
  txn->RegisterCommitAction([&](transaction::DeferredActionManager *) { committed = true; });

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

  auto *txn = txn_mgr_.BeginTransaction();

  auto insert_redo = txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, *pri);
  auto insert = insert_redo->Delta();

  bool aborted = false;
  bool committed = false;
  txn->RegisterAbortAction([&](transaction::DeferredActionManager *) { aborted = true; });
  txn->RegisterCommitAction([&](transaction::DeferredActionManager *) { committed = true; });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);

  auto *data = reinterpret_cast<int32_t *>(insert->AccessForceNotNull(pr_map->at(col_oid)));
  *data = 42;
  table.Insert(txn, insert_redo);

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);

  txn_mgr_.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  EXPECT_TRUE(committed);
  EXPECT_FALSE(aborted);

  insert = nullptr;
  delete pr_map;
  delete pri;
}

// Test that the GC performs available deferred actions when PerformGarbageCollection is called
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, SimpleDefer) {
  bool deferred = false;
  deferred_action_manager_.RegisterDeferredAction([&](transaction::timestamp_t) { deferred = true; });

  EXPECT_FALSE(deferred);

  EXPECT_EQ(1, deferred_action_manager_.Process());
  EXPECT_TRUE(deferred);
}

// Test that the GC correctly delays execution of deferred actions until the
// epoch (oldest running transaction) is greater than or equal to the next
// available timestamp when the action was deferred.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, DelayedDefer) {
  auto *txn = txn_mgr_.BeginTransaction();

  bool deferred = false;
  deferred_action_manager_.RegisterDeferredAction([&](transaction::timestamp_t) { deferred = true; });

  EXPECT_FALSE(deferred);

  EXPECT_EQ(0, deferred_action_manager_.Process());

  EXPECT_FALSE(deferred);  // txn is still open

  txn_mgr_.Abort(txn);

  EXPECT_EQ(1, deferred_action_manager_.Process());

  EXPECT_TRUE(deferred);
}

// Test that a deferred action can successfully generate and insert another
// deferred action (e.g. an "unlink" action could generate the paired "delete")
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, ChainedDefer) {
  bool defer1 = false;
  bool defer2 = false;

  deferred_action_manager_.RegisterDeferredAction([&](transaction::timestamp_t) {
    defer1 = true;
    deferred_action_manager_.RegisterDeferredAction([&](transaction::timestamp_t) { defer2 = true; });
  });

  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  EXPECT_EQ(1, deferred_action_manager_.Process());

  EXPECT_TRUE(defer1);
  EXPECT_FALSE(defer2);  // Sitting in txn_mgr_'s deferral queue

  EXPECT_EQ(1, deferred_action_manager_.Process());

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

  txn->RegisterCommitAction([&](transaction::DeferredActionManager *) { committed = true; });

  // Bootstrap into the lamda.  Need to eliminate the reference to txn since
  // it could get garbage collected before the lambda derefernces it.
  txn->RegisterAbortAction([&](transaction::DeferredActionManager *deferred_action_manager) {
    aborted = true;
    deferred_action_manager->RegisterDeferredAction([&, deferred_action_manager](transaction::timestamp_t) {
      defer1 = true;
      deferred_action_manager->RegisterDeferredAction([&](transaction::timestamp_t) { defer2 = true; });
    });
  });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  EXPECT_EQ(0, deferred_action_manager_.Process());

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  txn_mgr_.Abort(txn);

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  EXPECT_EQ(1, deferred_action_manager_.Process());

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_FALSE(defer2);

  EXPECT_EQ(1, deferred_action_manager_.Process());

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

  auto *txn = txn_mgr_.BeginTransaction();

  auto insert_redo = txn->StageWrite(CatalogTestUtil::test_db_oid, CatalogTestUtil::test_table_oid, *pri);
  auto insert = insert_redo->Delta();

  bool defer1 = false;
  bool defer2 = false;
  bool aborted = false;
  bool committed = false;

  txn->RegisterAbortAction([&](transaction::DeferredActionManager *) { aborted = true; });

  // Bootstrap into the lamda.  Need to eliminate the reference to txn since
  // it could get garbage collected before the lambda derefernces it.
  txn->RegisterCommitAction([&](transaction::DeferredActionManager *deferred_action_manager) {
    committed = true;
    deferred_action_manager->RegisterDeferredAction([&, deferred_action_manager](transaction::timestamp_t) {
      defer1 = true;
      deferred_action_manager->RegisterDeferredAction([&](transaction::timestamp_t) { defer2 = true; });
    });
  });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  auto *data = reinterpret_cast<int32_t *>(insert->AccessForceNotNull(pr_map->at(col_oid)));
  *data = 42;
  table.Insert(txn, insert_redo);

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  EXPECT_EQ(0, deferred_action_manager_.Process());

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  txn_mgr_.Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  EXPECT_FALSE(aborted);
  EXPECT_TRUE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  EXPECT_EQ(1, deferred_action_manager_.Process());

  EXPECT_FALSE(aborted);
  EXPECT_TRUE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_FALSE(defer2);

  EXPECT_EQ(1, deferred_action_manager_.Process());

  EXPECT_FALSE(aborted);
  EXPECT_TRUE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_TRUE(defer2);

  insert = nullptr;
  delete pr_map;
  delete pri;
}
}  // namespace terrier
