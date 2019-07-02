#pragma once

#include <memory>
#include <utility>

#include "gtest/gtest.h"

#include "execution/tpl_test.h"  // NOLINT

#include "execution/exec/execution_context.h"
#include "execution/sql/table_generator/table_generator.h"

namespace tpl {

class SqlBasedTest : public TplTest {
 public:
  SqlBasedTest() : TplTest() {}

  void SetUp() override {
    // NOTE: Do not move these into the constructor unless you change the loggers' initialization first.
    // Some of these objects use the loggers in their constructor (I know the catalog does), so they need to be
    // initialized after the loggers.
    TplTest::SetUp();
    // Initialize terrier objects
    block_store_ = std::make_unique<terrier::storage::BlockStore>(1000, 1000);
    buffer_pool_ = std::make_unique<terrier::storage::RecordBufferSegmentPool>(100000, 100000);
    log_manager_ = std::make_unique<terrier::storage::LogManager>("log_file.log", buffer_pool_.get());
    txn_manager_ =
        std::make_unique<terrier::transaction::TransactionManager>(buffer_pool_.get(), false, log_manager_.get());
    test_txn_ = txn_manager_->BeginTransaction();

    // Create catalog and exec ctx
    catalog_ = std::make_unique<terrier::catalog::Catalog>(txn_manager_.get(), test_txn_);
    test_ns_oid_ = catalog_->CreateNameSpace(test_txn_, test_db_oid_, "test_ns");
  }

  ~SqlBasedTest() override {
    txn_manager_->Commit(test_txn_, [](void *) {}, nullptr);
    delete test_txn_;
    log_manager_->Shutdown();
  }

  std::unique_ptr<exec::ExecutionContext> MakeExecCtx(exec::OutputCallback &&callback = nullptr,
                                                      const terrier::planner::OutputSchema *schema = nullptr) {
    auto accessor = catalog_->GetAccessor(test_txn_, test_db_oid_, test_ns_oid_);
    return std::make_unique<exec::ExecutionContext>(test_txn_, callback, schema, std::move(accessor));
  }

  std::unique_ptr<terrier::catalog::CatalogAccessor> MakeAccessor() {
    return catalog_->GetAccessor(test_txn_, test_db_oid_, test_ns_oid_);
  }

 private:
  std::unique_ptr<terrier::storage::BlockStore> block_store_;
  std::unique_ptr<terrier::storage::RecordBufferSegmentPool> buffer_pool_;
  std::unique_ptr<terrier::storage::LogManager> log_manager_;
  std::unique_ptr<terrier::transaction::TransactionManager> txn_manager_;
  std::unique_ptr<terrier::catalog::Catalog> catalog_;
  terrier::transaction::TransactionContext *test_txn_;
  terrier::catalog::db_oid_t test_db_oid_ = terrier::catalog::DEFAULT_DATABASE_OID;
  terrier::catalog::namespace_oid_t test_ns_oid_;
};

}  // namespace tpl
