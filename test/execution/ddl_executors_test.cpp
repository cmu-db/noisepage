#include "execution/sql/ddl_executors.h"

#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "main/db_main.h"
#include "planner/plannodes/alter_plan_node.h"
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
    db_main_ = terrier::DBMain::Builder().SetUseGC(true).SetUseCatalog(true).Build();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
    auto *txn = txn_manager_->BeginTransaction();
    db_ = catalog_->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
    txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

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

    txn_ = txn_manager_->BeginTransaction();
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_);
  }

  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  common::ManagedPointer<storage::BlockStore> block_store_;

  catalog::db_oid_t db_;

  std::unique_ptr<catalog::Schema> table_schema_;
  std::unique_ptr<catalog::IndexSchema> index_schema_;

  transaction::TransactionContext *txn_;
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
};

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateDatabasePlanNode) {
  planner::CreateDatabasePlanNode::Builder builder;
  auto create_db_node = builder.SetDatabaseName("foo").Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateDatabaseExecutor(
      common::ManagedPointer<planner::CreateDatabasePlanNode>(create_db_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto db_oid = accessor_->GetDatabaseOid("foo");
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateDatabasePlanNodeNameConflict) {
  planner::CreateDatabasePlanNode::Builder builder;
  auto create_db_node = builder.SetDatabaseName("foo").Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateDatabaseExecutor(
      common::ManagedPointer<planner::CreateDatabasePlanNode>(create_db_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto db_oid = accessor_->GetDatabaseOid("foo");
  EXPECT_NE(db_oid, catalog::INVALID_DATABASE_OID);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateDatabaseExecutor(
      common::ManagedPointer<planner::CreateDatabasePlanNode>(create_db_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateNamespacePlanNode) {
  planner::CreateNamespacePlanNode::Builder builder;
  auto create_ns_node = builder.SetNamespaceName("foo").Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateNamespaceExecutor(
      common::ManagedPointer<planner::CreateNamespacePlanNode>(create_ns_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto ns_oid = accessor_->GetNamespaceOid("foo");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateNamespacePlanNodeNameConflict) {
  planner::CreateNamespacePlanNode::Builder builder;
  auto create_ns_node = builder.SetNamespaceName("foo").Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateNamespaceExecutor(
      common::ManagedPointer<planner::CreateNamespacePlanNode>(create_ns_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto ns_oid = accessor_->GetNamespaceOid("foo");
  EXPECT_NE(ns_oid, catalog::INVALID_NAMESPACE_OID);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateNamespaceExecutor(
      common::ManagedPointer<planner::CreateNamespacePlanNode>(create_ns_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNode) {
  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodeAbort) {
  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateTablePlanNodeTableNameConflict) {
  planner::CreateTablePlanNode::Builder builder;
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  txn_manager_->Abort(txn_);
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
                               .SetBlockStore(block_store_)
                               .SetHasPrimaryKey(true)
                               .SetPrimaryKey(std::move(pk_info))
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo_pkey");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
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
                               .SetBlockStore(block_store_)
                               .SetHasPrimaryKey(true)
                               .SetPrimaryKey(std::move(pk_info))
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo_pkey");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Abort(txn_);
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
                               .SetBlockStore(block_store_)
                               .SetHasPrimaryKey(true)
                               .SetPrimaryKey(std::move(pk_info))
                               .Build();
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  EXPECT_EQ(index_oid, catalog::INVALID_INDEX_OID);
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateIndexPlanNode) {
  planner::CreateIndexPlanNode::Builder builder;
  auto create_index_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableOid(CatalogTestUtil::TEST_TABLE_OID)
                               .SetSchema(std::move(index_schema_))
                               .SetIndexName("foo")
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  auto index_ptr = accessor_->GetIndex(index_oid);
  EXPECT_NE(index_ptr, nullptr);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateIndexPlanNodeAbort) {
  planner::CreateIndexPlanNode::Builder builder;
  auto create_index_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableOid(CatalogTestUtil::TEST_TABLE_OID)
                               .SetSchema(std::move(index_schema_))
                               .SetIndexName("foo")
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  auto index_ptr = accessor_->GetIndex(index_oid);
  EXPECT_NE(index_ptr, nullptr);
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, CreateIndexPlanNodeIndexNameConflict) {
  planner::CreateIndexPlanNode::Builder builder;
  auto create_index_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableOid(CatalogTestUtil::TEST_TABLE_OID)
                               .SetSchema(std::move(index_schema_))
                               .SetIndexName("foo")
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  auto index_oid = accessor_->GetIndexOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(index_oid, catalog::INVALID_INDEX_OID);
  auto index_ptr = accessor_->GetIndex(index_oid);
  EXPECT_NE(index_ptr, nullptr);
  EXPECT_FALSE(execution::sql::DDLExecutors::CreateIndexExecutor(
      common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));
  txn_manager_->Abort(txn_);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, DropTablePlanNode) {
  planner::CreateTablePlanNode::Builder create_builder;
  auto create_table_node = create_builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);

  planner::DropTablePlanNode::Builder drop_builder;
  auto drop_table_node = drop_builder.SetTableOid(table_oid).Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::DropTableExecutor(
      common::ManagedPointer<planner::DropTablePlanNode>(drop_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));

  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
}

// NOLINTNEXTLINE
TEST_F(DDLExecutorsTests, AlterTablePlanNodeAddColumn) {
  // TODO(SC): alter table command tests moving to another test file?

  // Create table
  planner::CreateTablePlanNode::Builder builder;
  catalog::Schema original_schema(table_schema_->GetColumns());
  auto create_table_node = builder.SetNamespaceOid(CatalogTestUtil::TEST_NAMESPACE_OID)
                               .SetTableSchema(std::move(table_schema_))
                               .SetTableName("foo")
                               .SetBlockStore(block_store_)
                               .Build();
  EXPECT_TRUE(execution::sql::DDLExecutors::CreateTableExecutor(
      common::ManagedPointer<planner::CreateTablePlanNode>(create_table_node),
      common::ManagedPointer<catalog::CatalogAccessor>(accessor_), db_));
  auto table_oid = accessor_->GetTableOid(CatalogTestUtil::TEST_NAMESPACE_OID, "foo");
  EXPECT_NE(table_oid, catalog::INVALID_TABLE_OID);
  auto table_ptr = accessor_->GetTable(table_oid);
  EXPECT_NE(table_ptr, nullptr);
  EXPECT_EQ(accessor_->GetColumns(table_oid).size(), original_schema.GetColumns().size());
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);

  // Add a column
  txn_ = txn_manager_->BeginTransaction();
  accessor_ = catalog_->GetAccessor(common::ManagedPointer(txn_), db_);

  planner::AlterPlanNode::Builder alter_builder;
  auto default_val = parser::ConstantValueExpression(type::TransientValueFactory::GetInteger(15712));
  catalog::Schema::Column col("new_column", type::TypeId::INTEGER, false, default_val);
  std::vector<std::unique_ptr<planner::AlterCmdBase>> cmds;
  auto add_cmd = std::make_unique<planner::AlterPlanNode::AddColumnCmd>(std::move(col), nullptr, nullptr, nullptr);
  cmds.push_back(std::move(add_cmd));
  auto alter_table_node = alter_builder.SetTableOid(table_oid)
                              .SetCommands(std::move(cmds))
                              .SetColumnOIDs({catalog::INVALID_COLUMN_OID})
                              .Build();
  EXPECT_TRUE(
      execution::sql::DDLExecutors::AlterTableExecutor(common::ManagedPointer<planner::AlterPlanNode>(alter_table_node),
                                                       common::ManagedPointer<catalog::CatalogAccessor>(accessor_)));

  EXPECT_EQ(accessor_->GetColumns(table_oid).size(), original_schema.GetColumns().size() + 1);
  auto &cur_schema = accessor_->GetSchema(table_oid);
  EXPECT_EQ(cur_schema.GetColumns().size(), original_schema.GetColumns().size() + 1);
  EXPECT_NO_THROW(cur_schema.GetColumn("new_column"));
  auto &new_col = cur_schema.GetColumn("new_column");
  EXPECT_FALSE(new_col.Nullable());
  auto stored_default_val = new_col.StoredExpression();
  EXPECT_EQ(stored_default_val->GetReturnValueType(), type::TypeId::INTEGER);
  auto val = stored_default_val.CastManagedPointerTo<const parser::ConstantValueExpression>()->GetValue();
  EXPECT_EQ(type::TransientValuePeeker::PeekInteger(val), 15712);
  txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);

  // SQL table related (Should we test it here?)
  // EXPECT_NO_THROW(table_ptr->GetBlockLayout(storage::layout_version_t(1)));
  // auto block_layout = table_ptr->GetBlockLayout(storage::layout_version_t(1));
  // EXPECT_EQ(block_layout.NumColumns(), cur_schema.GetColumns().size());
  // EXPECT_NO_THROW(table_ptr->GetColumnOidToIdMap(storage::layout_version_t(1)));
  // auto col_oid_id_map = table_ptr->GetColumnOidToIdMap(storage::layout_version_t(1));
  // EXPECT_EQ(block_layout.AttrSize(col_oid_id_map[col.Oid()]), type::TypeUtil::GetTypeSize(type::TypeId::INTEGER));
}

}  // namespace terrier::execution::sql::test
