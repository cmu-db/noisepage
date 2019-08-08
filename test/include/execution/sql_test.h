#pragma once

#include <memory>
#include <utility>

#include "gtest/gtest.h"

#include "execution/tpl_test.h"

#include "execution/exec/execution_context.h"
#include "execution/sql/table_generator/table_generator.h"
#include "storage/garbage_collector.h"

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
    block_store_ = std::make_unique<storage::BlockStore>(1000, 1000);
    buffer_pool_ = std::make_unique<storage::RecordBufferSegmentPool>(100000, 100000);
    txn_manager_ = std::make_unique<transaction::TransactionManager>(buffer_pool_.get(), true, nullptr);
    gc_ = std::make_unique<storage::GarbageCollector>(txn_manager_.get(), nullptr);
    test_txn_ = txn_manager_->BeginTransaction();

    // Create catalog and test namespace
    catalog_ = std::make_unique<catalog::Catalog>(txn_manager_.get(), block_store_.get());
    test_db_oid_ = catalog_->CreateDatabase(test_txn_, "test_db", true);
    ASSERT_NE(test_db_oid_, catalog::INVALID_DATABASE_OID) << "Default database does not exist";
    auto accessor = catalog_->GetAccessor(test_txn_, test_db_oid_);
    test_ns_oid_ = accessor->CreateNamespace("test_ns");
  }

  ~SqlBasedTest() override {
    txn_manager_->Commit(test_txn_, [](void *) {}, nullptr);
    catalog_->TearDown();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
  }

  catalog::namespace_oid_t NSOid() { return test_ns_oid_; }

  storage::BlockStore *BlockStore() { return block_store_.get(); }

  std::unique_ptr<exec::ExecutionContext> MakeExecCtx(exec::OutputCallback &&callback = nullptr,
                                                      const planner::OutputSchema *schema = nullptr) {
    auto accessor = catalog_->GetAccessor(test_txn_, test_db_oid_);
    return std::make_unique<exec::ExecutionContext>(test_db_oid_, test_txn_, callback, schema, std::move(accessor));
  }

  void GenerateTestTables(exec::ExecutionContext *exec_ctx) {
    sql::TableGenerator table_generator{exec_ctx, block_store_.get(), test_ns_oid_};
    table_generator.GenerateTestTables();
  }

  parser::ConstantValueExpression DummyCVE() {
    return parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(0));
  }

 private:
  std::unique_ptr<storage::BlockStore> block_store_;
  std::unique_ptr<storage::RecordBufferSegmentPool> buffer_pool_;
  std::unique_ptr<transaction::TransactionManager> txn_manager_;
  std::unique_ptr<catalog::Catalog> catalog_;
  std::unique_ptr<storage::GarbageCollector> gc_;
  catalog::db_oid_t test_db_oid_{0};
  catalog::namespace_oid_t test_ns_oid_;
  transaction::TransactionContext *test_txn_;
};

}  // namespace terrier::execution
