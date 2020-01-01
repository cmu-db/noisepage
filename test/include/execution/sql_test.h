#pragma once

#include <memory>
#include <utility>

#include "execution/exec/execution_context.h"
#include "execution/table_generator/table_generator.h"
#include "execution/tpl_test.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "storage/garbage_collector.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/timestamp_manager.h"

namespace terrier::execution {

class SqlBasedTest : public TplTest {
 public:
  SqlBasedTest() = default;

  void SetUp() override {
    // NOTE: Do not move these into the constructor unless you change the loggers' initialization first.
    // Some of these objects use the loggers in their constructor (I know the catalog does), so they need to be
    // initialized after the loggers.
    TplTest::SetUp();
    // Initialize terrier objects

    db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseGCThread(true).SetUseCatalog(true).Build();

    block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();

    test_txn_ = txn_manager_->BeginTransaction();

    // Create catalog and test namespace
    test_db_oid_ = catalog_->CreateDatabase(test_txn_, "test_db", true);
    ASSERT_NE(test_db_oid_, catalog::INVALID_DATABASE_OID) << "Default database does not exist";
    auto accessor = catalog_->GetAccessor(test_txn_, test_db_oid_);
    test_ns_oid_ = accessor->GetDefaultNamespace();
  }

  ~SqlBasedTest() override { txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr); }

  catalog::namespace_oid_t NSOid() { return test_ns_oid_; }

  storage::BlockStore *BlockStore() { return block_store_.Get(); }

  std::unique_ptr<exec::ExecutionContext> MakeExecCtx(exec::OutputCallback &&callback = nullptr,
                                                      const planner::OutputSchema *schema = nullptr) {
    auto accessor = catalog_->GetAccessor(test_txn_, test_db_oid_);
    return std::make_unique<exec::ExecutionContext>(test_db_oid_, test_txn_, callback, schema, std::move(accessor));
  }

  void GenerateTestTables(exec::ExecutionContext *exec_ctx) {
    sql::TableGenerator table_generator{exec_ctx, block_store_.Get(), test_ns_oid_};
    table_generator.GenerateTestTables(false);
  }

  parser::ConstantValueExpression DummyCVE() {
    return parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
  }

  std::unique_ptr<terrier::catalog::CatalogAccessor> MakeAccessor() {
    return catalog_->GetAccessor(test_txn_, test_db_oid_);
  }

 private:
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  catalog::db_oid_t test_db_oid_{0};
  catalog::namespace_oid_t test_ns_oid_;
  transaction::TransactionContext *test_txn_;
};

}  // namespace terrier::execution
