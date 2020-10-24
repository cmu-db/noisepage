#include <memory>
#include <vector>

#include "main/db_main.h"
#include "storage/garbage_collector.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "test_util/catalog_test_util.h"
#include "test_util/data_table_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "transaction/transaction_manager.h"

namespace noisepage {

class DeferredActionsTest : public TerrierTest {
 protected:
  void SetUp() override {
    db_main_ = noisepage::DBMain::Builder().SetUseGC(true).Build();
    txn_mgr_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    deferred_action_manager_ = db_main_->GetTransactionLayer()->GetDeferredActionManager();
    gc_ = db_main_->GetStorageLayer()->GetGarbageCollector();
  }

  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<transaction::TransactionManager> txn_mgr_;
  common::ManagedPointer<transaction::DeferredActionManager> deferred_action_manager_;
  common::ManagedPointer<storage::GarbageCollector> gc_;
};

// Test that abort actions do not execute before the transaction aborts and that
// commit actions are never executed.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, AbortAction) {
  auto *txn = txn_mgr_->BeginTransaction();

  bool aborted = false;
  bool committed = false;
  txn->RegisterAbortAction([&]() { aborted = true; });
  txn->RegisterCommitAction([&]() { committed = true; });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);

  txn_mgr_->Abort(txn);

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
}

// Test that commit actions are not executed before the transaction commits and
// that abort actions are never executed.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, CommitAction) {
  auto *txn = txn_mgr_->BeginTransaction();

  bool aborted = false;
  bool committed = false;
  txn->RegisterAbortAction([&]() { aborted = true; });
  txn->RegisterCommitAction([&]() { committed = true; });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);

  txn_mgr_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  EXPECT_TRUE(committed);
  EXPECT_FALSE(aborted);

  gc_->PerformGarbageCollection();
  gc_->PerformGarbageCollection();
}

// Test that the GC performs available deferred actions when PerformGarbageCollection is called
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, SimpleDefer) {
  bool deferred = false;
  deferred_action_manager_->RegisterDeferredAction([&]() { deferred = true; });

  EXPECT_FALSE(deferred);

  gc_->PerformGarbageCollection();

  EXPECT_TRUE(deferred);
}

// Test that the GC correctly delays execution of deferred actions until the
// epoch (oldest running transaction) is greater than or equal to the next
// available timestamp when the action was deferred.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, DelayedDefer) {
  auto *txn = txn_mgr_->BeginTransaction();

  bool deferred = false;
  deferred_action_manager_->RegisterDeferredAction([&]() { deferred = true; });

  EXPECT_FALSE(deferred);

  gc_->PerformGarbageCollection();

  EXPECT_FALSE(deferred);  // txn is still open

  txn_mgr_->Abort(txn);

  gc_->PerformGarbageCollection();

  EXPECT_TRUE(deferred);
}

// Test that a deferred action can successfully generate and insert another
// deferred action (e.g. an "unlink" action could generate the paired "delete")
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, ChainedDefer) {
  bool defer1 = false;
  bool defer2 = false;
  deferred_action_manager_->RegisterDeferredAction([&]() {
    defer1 = true;
    deferred_action_manager_->RegisterDeferredAction([&]() { defer2 = true; });
  });

  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_->PerformGarbageCollection();

  EXPECT_TRUE(defer1);
  EXPECT_FALSE(defer2);  // Sitting in txn_mgr_'s deferral queue

  gc_->PerformGarbageCollection();

  EXPECT_TRUE(defer1);
  EXPECT_TRUE(defer2);
}

// Test that the transaction context's interface supports creating a deep deferral
// chain that conditionally executes only on abort.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, AbortBootstrapDefer) {
  auto *txn = txn_mgr_->BeginTransaction();

  bool defer1 = false;
  bool defer2 = false;
  bool aborted = false;
  bool committed = false;

  txn->RegisterCommitAction([&]() { committed = true; });
  txn->RegisterAbortAction([&](transaction::DeferredActionManager *deferred_action_manager) {
    aborted = true;
    deferred_action_manager->RegisterDeferredAction([&, deferred_action_manager]() {
      defer1 = true;
      deferred_action_manager->RegisterDeferredAction([&]() { defer2 = true; });
    });
  });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_->PerformGarbageCollection();

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  txn_mgr_->Abort(txn);

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_->PerformGarbageCollection();

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_FALSE(defer2);

  gc_->PerformGarbageCollection();

  EXPECT_TRUE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_TRUE(defer2);
}

// Test that the transaction context's interface supports creating a deep deferral
// chain that conditionally executes only on commit.
// NOLINTNEXTLINE
TEST_F(DeferredActionsTest, CommitBootstrapDefer) {
  auto *txn = txn_mgr_->BeginTransaction();

  bool defer1 = false;
  bool defer2 = false;
  bool aborted = false;
  bool committed = false;

  txn->RegisterAbortAction([&]() { aborted = true; });
  txn->RegisterCommitAction([&](transaction::DeferredActionManager *deferred_action_manager) {
    committed = true;
    deferred_action_manager->RegisterDeferredAction([&, deferred_action_manager]() {
      defer1 = true;
      deferred_action_manager->RegisterDeferredAction([&]() { defer2 = true; });
    });
  });

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_->PerformGarbageCollection();

  EXPECT_FALSE(aborted);
  EXPECT_FALSE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  txn_mgr_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  EXPECT_FALSE(aborted);
  EXPECT_TRUE(committed);
  EXPECT_FALSE(defer1);
  EXPECT_FALSE(defer2);

  gc_->PerformGarbageCollection();

  EXPECT_FALSE(aborted);
  EXPECT_TRUE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_FALSE(defer2);

  gc_->PerformGarbageCollection();

  EXPECT_FALSE(aborted);
  EXPECT_TRUE(committed);
  EXPECT_TRUE(defer1);
  EXPECT_TRUE(defer2);
}
}  // namespace noisepage
