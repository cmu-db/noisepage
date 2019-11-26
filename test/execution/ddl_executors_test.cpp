#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/ddl_executors.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "test_util/catalog_test_util.h"
#include "test_util/test_harness.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"
#include "transaction/transaction_util.h"

namespace terrier::execution::sql::test {

class DDLExecutorsTests : public TerrierTest {
 public:
  void SetUp() override {
    TerrierTest::SetUp();

    // Initialize the transaction manager and GC
    timestamp_manager_ = std::make_unique<transaction::TimestampManager>();
    deferred_action_manager_ = std::make_unique<transaction::DeferredActionManager>(timestamp_manager_.get());
    txn_manager_ = std::make_unique<transaction::TransactionManager>(
        timestamp_manager_.get(), deferred_action_manager_.get(), &buffer_pool_, true, DISABLED);
    gc_ = std::make_unique<storage::GarbageCollector>(timestamp_manager_.get(), deferred_action_manager_.get(),
                                                      txn_manager_.get(), nullptr);

    // Build out the catalog and commit so that it is visible to other transactions
    catalog_ = std::make_unique<catalog::Catalog>(txn_manager_.get(), &block_store_);

    auto *txn = txn_manager_->BeginTransaction();
    db_ = catalog_->CreateDatabase(txn, "terrier", true);
    EXPECT_NE(db_, catalog::INVALID_DATABASE_OID);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

    // Run the GC to flush it down to a clean system
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    auto col = catalog::Schema::Column(
        "attribute", type::TypeId::INTEGER, false,
        parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    StorageTestUtil::ForceOid(&(col), catalog::col_oid_t(1));
    table_schema_ = std::make_unique<catalog::Schema>(std::vector<catalog::Schema::Column>{col});

    std::vector<catalog::IndexSchema::Column> keycols;
    keycols.emplace_back("", type::TypeId::INTEGER, false,
                         parser::ColumnValueExpression(CatalogTestUtil::TEST_DB_OID, CatalogTestUtil::TEST_TABLE_OID,
                                                       catalog::col_oid_t(1)));
    StorageTestUtil::ForceOid(&(keycols[0]), catalog::indexkeycol_oid_t(1));
    index_schema_ =
        std::make_unique<catalog::IndexSchema>(keycols, storage::index::IndexType::BWTREE, true, true, false, true);
  }

  void TearDown() override {
    catalog_->TearDown();
    // Run the GC to clean up transactions
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();
    gc_->PerformGarbageCollection();

    TerrierTest::TearDown();
  }

  std::unique_ptr<catalog::Catalog> catalog_;
  storage::RecordBufferSegmentPool buffer_pool_{100, 100};
  storage::BlockStore block_store_{100, 100};
  std::unique_ptr<storage::GarbageCollector> gc_;
  std::unique_ptr<transaction::TransactionManager> txn_manager_;
  std::unique_ptr<transaction::DeferredActionManager> deferred_action_manager_;
  std::unique_ptr<transaction::TimestampManager> timestamp_manager_;

  catalog::db_oid_t db_;

  std::unique_ptr<catalog::Schema> table_schema_;
  std::unique_ptr<catalog::IndexSchema> index_schema_;

  auto CreateExecutionContext() const {
    auto *txn = txn_manager_->BeginTransaction();
    auto accessor = catalog_->GetAccessor(txn, db_);
    return std::make_unique<execution::exec::ExecutionContext>(db_, txn, nullptr, nullptr, std::move(accessor));
  }
};

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateDatabasePlanNode) {
  planner::CreateDatabasePlanNode::Builder builder;
  auto create_db_node = builder.SetDatabaseName("foo").Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateDatabaseExecutor(
      common::ManagedPointer<planner::CreateDatabasePlanNode>(create_db_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto db_oid = accessor->GetDatabaseOid("foo");
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  txn_manager_->Commit(exec_ctx->GetTxn(), transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateDatabasePlanNodeNameConflict) {
  planner::CreateDatabasePlanNode::Builder builder;
  auto create_db_node = builder.SetDatabaseName("foo").Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateDatabaseExecutor(
      common::ManagedPointer<planner::CreateDatabasePlanNode>(create_db_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto db_oid = accessor->GetDatabaseOid("foo");
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateDatabaseExecutor(
      common::ManagedPointer<planner::CreateDatabasePlanNode>(create_db_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  txn_manager_->Abort(exec_ctx->GetTxn());
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateNamespacePlanNode) {
  planner::CreateNamespacePlanNode::Builder builder;
  auto create_ns_node = builder.SetNamespaceName("foo").Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateNamespaceExecutor(
      common::ManagedPointer<planner::CreateNamespacePlanNode>(create_ns_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto ns_oid = accessor->GetNamespaceOid("foo");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  txn_manager_->Commit(exec_ctx->GetTxn(), transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateNamespacePlanNodeNameConflict) {
  planner::CreateNamespacePlanNode::Builder builder;
  auto create_ns_node = builder.SetNamespaceName("foo").Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateNamespaceExecutor(
      common::ManagedPointer<planner::CreateNamespacePlanNode>(create_ns_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto ns_oid = accessor->GetNamespaceOid("foo");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateNamespaceExecutor(
      common::ManagedPointer<planner::CreateNamespacePlanNode>(create_ns_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  txn_manager_->Abort(exec_ctx->GetTxn());
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNode) {
  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(common::ManagedPointer<storage::BlockStore>(&block_store_))
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto table_oid = accessor->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  txn_manager_->Commit(exec_ctx->GetTxn(), transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodeAbort) {
  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(common::ManagedPointer<storage::BlockStore>(&block_store_))
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto table_oid = accessor->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  txn_manager_->Abort(exec_ctx->GetTxn());
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodeTableNameConflict) {
  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(common::ManagedPointer<storage::BlockStore>(&block_store_))
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto table_oid = accessor->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  txn_manager_->Abort(exec_ctx->GetTxn());
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodePKey) {
  planner::PrimaryKeyInfo pk_info;
  pk_info.primary_key_cols_ = {"attribute"};
  pk_info.constraint_name_ = "foo_pkey";

  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(common::ManagedPointer<storage::BlockStore>(&block_store_))
                               .SetHasPrimaryKey(true)
                               .SetPrimaryKey(std::move(pk_info))
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto table_oid = accessor->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  auto index_oid = accessor->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo_pkey");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Commit(exec_ctx->GetTxn(), transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodePKeyAbort) {
  planner::PrimaryKeyInfo pk_info;
  pk_info.primary_key_cols_ = {"attribute"};
  pk_info.constraint_name_ = "foo_pkey";

  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(common::ManagedPointer<storage::BlockStore>(&block_store_))
                               .SetHasPrimaryKey(true)
                               .SetPrimaryKey(std::move(pk_info))
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto table_oid = accessor->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  auto index_oid = accessor->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo_pkey");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Abort(exec_ctx->GetTxn());
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodePKeyNameConflict) {
  planner::PrimaryKeyInfo pk_info;
  pk_info.primary_key_cols_ = {"attribute"};
  pk_info.constraint_name_ = "foo";

  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(common::ManagedPointer<storage::BlockStore>(&block_store_))
                               .SetHasPrimaryKey(true)
                               .SetPrimaryKey(std::move(pk_info))
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto table_oid = accessor->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  auto index_oid = accessor->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_EQ(index_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Abort(exec_ctx->GetTxn());
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateIndexPlanNode) {
  planner::CreateIndexPlanNode::Builder builder;
  auto create_index_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableOid(CatalogTestUtil::TEST_TABLE_OID)
                               .SetSchema(std::move(index_schema_))
                               .SetIndexName("foo")
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto index_oid = accessor->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  auto index_ptr = accessor->GetIndex(index_oid);
  EXPECT_NE(index_ptr, nullptr);
  txn_manager_->Commit(exec_ctx->GetTxn(), transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateIndexPlanNodeAbort) {
  planner::CreateIndexPlanNode::Builder builder;
  auto create_index_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableOid(CatalogTestUtil::TEST_TABLE_OID)
                               .SetSchema(std::move(index_schema_))
                               .SetIndexName("foo")
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto index_oid = accessor->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  auto index_ptr = accessor->GetIndex(index_oid);
  EXPECT_NE(index_ptr, nullptr);
  txn_manager_->Abort(exec_ctx->GetTxn());
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateIndexPlanNodeIndexNameConflict) {
  planner::CreateIndexPlanNode::Builder builder;
  auto create_index_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableOid(CatalogTestUtil::TEST_TABLE_OID)
                               .SetSchema(std::move(index_schema_))
                               .SetIndexName("foo")
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto index_oid = accessor->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  auto index_ptr = accessor->GetIndex(index_oid);
  EXPECT_NE(index_ptr, nullptr);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  txn_manager_->Abort(exec_ctx->GetTxn());
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, DropTablePlanNode) {
  planner::CreateTablePlanNode::Builder create_builder;
  auto create_table_node = create_builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(common::ManagedPointer<storage::BlockStore>(&block_store_))
                               .Build();
  auto exec_ctx = CreateExecutionContext();
  auto accessor = exec_ctx->GetAccessor();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));
  auto table_oid = accessor->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);

  planner::DropTablePlanNode::Builder drop_builder;
  auto drop_table_node = drop_builder.SetTableOid(table_oid).Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::DropTableExecutor(
      common::ManagedPointer<planner::DropTablePlanNode>(drop_table_node),
      common::ManagedPointer<exec::ExecutionContext>(exec_ctx)));

  txn_manager_->Commit(exec_ctx->GetTxn(), transaction::TransactionUtil::EmptyCallback, nullptr);
}

}  // namespace terrier::execution::sql::test
