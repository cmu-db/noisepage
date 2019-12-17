#include "execution/sql/ddl_executors.h"
#include <memory>
#include <string>
#include <vector>
#include "catalog/catalog_accessor.h"
#include "common/macros.h"
#include "execution/exec/execution_context.h"
#include "parser/expression/column_value_expression.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"
#include "type/transient_value_factory.h"

namespace terrier::execution::sql {

bool DDLExecutors::CreateDatabaseExecutor(const common::ManagedPointer<planner::CreateDatabasePlanNode> node,
                                          const common::ManagedPointer<exec::ExecutionContext> context) {
  auto *const accessor = context->GetAccessor();
  // Request permission from the Catalog to see if this a valid database name
  return accessor->CreateDatabase(node->GetDatabaseName()) != catalog::INVALID_DATABASE_OID;
}

bool DDLExecutors::CreateNamespaceExecutor(const common::ManagedPointer<planner::CreateNamespacePlanNode> node,
                                           const common::ManagedPointer<exec::ExecutionContext> context) {
  auto *const accessor = context->GetAccessor();
  // Request permission from the Catalog to see if this a valid namespace name
  return accessor->CreateNamespace(node->GetNamespaceName()) != catalog::INVALID_NAMESPACE_OID;
}

bool DDLExecutors::CreateTableExecutor(const common::ManagedPointer<planner::CreateTablePlanNode> node,
                                       const common::ManagedPointer<exec::ExecutionContext> context) {
  auto *const accessor = context->GetAccessor();
  // Request permission from the Catalog to see if this a valid namespace and table name
  const auto table_oid = accessor->CreateTable(node->GetNamespaceOid(), node->GetTableName(), *(node->GetSchema()));
  if (table_oid == catalog::INVALID_TABLE_OID) {
    // Catalog wasn't able to proceed, txn must now abort
    return false;
  }
  // Get the canonical Schema from the Catalog now that column oids have been assigned
  const auto &schema = accessor->GetSchema(table_oid);
  // Instantiate a SqlTable and update the pointer in the Catalog
  auto *const table = new storage::SqlTable(node->GetBlockStore().Get(), schema);
  bool result UNUSED_ATTRIBUTE = accessor->SetTablePointer(table_oid, table);
  TERRIER_ASSERT(result, "CreateTable succeeded, SetTablePointer must also succeed.");

  if (node->HasPrimaryKey()) {
    // Create the IndexSchema Columns by referencing the Columns in the canonical table Schema from the Catalog
    std::vector<catalog::IndexSchema::Column> key_cols;
    const auto &primary_key_info = node->GetPrimaryKey();
    key_cols.reserve(primary_key_info.primary_key_cols_.size());
    for (const auto &parser_col : primary_key_info.primary_key_cols_) {
      const auto &table_col = schema.GetColumn(parser_col);
      if (table_col.Type() == type::TypeId::VARCHAR || table_col.Type() == type::TypeId::VARBINARY) {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.MaxVarlenSize(), table_col.Nullable(),
                              parser::ColumnValueExpression(context->DBOid(), table_oid, table_col.Oid()));

      } else {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.Nullable(),
                              parser::ColumnValueExpression(context->DBOid(), table_oid, table_col.Oid()));
      }
    }
    catalog::IndexSchema index_schema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);

    // Create the index, and use its return value as overall success result
    return CreateIndex(context, node->GetNamespaceOid(), primary_key_info.constraint_name_, table_oid, index_schema);
  }

  // TODO(Matt): interpret other fields in CreateTablePlanNode when we support them in the Catalog:
  // foreign_keys_, con_uniques_, con_checks_,

  return true;
}

bool DDLExecutors::CreateIndexExecutor(const common::ManagedPointer<planner::CreateIndexPlanNode> node,
                                       const common::ManagedPointer<exec::ExecutionContext> context) {
  return CreateIndex(context, node->GetNamespaceOid(), node->GetIndexName(), node->GetTableOid(), *(node->GetSchema()));
}

bool DDLExecutors::DropDatabaseExecutor(const common::ManagedPointer<planner::DropDatabasePlanNode> node,
                                        const common::ManagedPointer<exec::ExecutionContext> context) {
  TERRIER_ASSERT(context->DBOid() != node->GetDatabaseOid(),
                 "This command cannot be executed while connected to the target database. This should be checked in "
                 "the binder.");
  auto *const accessor = context->GetAccessor();
  const bool result = accessor->DropDatabase(node->GetDatabaseOid());
  return result;
}

bool DDLExecutors::DropNamespaceExecutor(const common::ManagedPointer<planner::DropNamespacePlanNode> node,
                                         const common::ManagedPointer<exec::ExecutionContext> context) {
  auto *const accessor = context->GetAccessor();
  const bool result = accessor->DropNamespace(node->GetNamespaceOid());
  // TODO(Matt): CASCADE?
  return result;
}

bool DDLExecutors::DropTableExecutor(const common::ManagedPointer<planner::DropTablePlanNode> node,
                                     const common::ManagedPointer<exec::ExecutionContext> context) {
  auto *const accessor = context->GetAccessor();
  const bool result = accessor->DropTable(node->GetTableOid());
  // TODO(Matt): CASCADE?
  return result;
}

bool DDLExecutors::DropIndexExecutor(const common::ManagedPointer<planner::DropIndexPlanNode> node,
                                     const common::ManagedPointer<exec::ExecutionContext> context) {
  auto *const accessor = context->GetAccessor();
  const bool result = accessor->DropIndex(node->GetIndexOid());
  // TODO(Matt): CASCADE?
  return result;
}

bool DDLExecutors::PrepareExecutor(const common::ManagedPointer<planner::PreparePlanNode> node,
                                   const common::ManagedPointer<exec::ExecutionContext> context) {
  auto *const accessor = context->GetAccessor();
  auto temp_namespace = accessor->GetTempNamespace();

  // Check if there is already a table for storing Prepared Statements in the temporary namespace
  auto table_oid = accessor->GetTableOid(temp_namespace, "pg_prepare");
  if (table_oid == catalog::INVALID_TABLE_OID) {
    // Create the column definition for storing Prepared Statements
    std::vector<catalog::Schema::Column> cols;
    cols.emplace_back("name", type::TypeId::VARCHAR, false,
                      parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    cols.emplace_back("prepare_plan", type::TypeId::BIGINT, false,
                      parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    cols.emplace_back("execute_plan", type::TypeId::BIGINT, false,
                      parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    cols.emplace_back("compiled_plan", type::TypeId::BIGINT, false,
                      parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER)));
    auto schema = catalog::Schema(cols);

    table_oid = accessor->CreateTable(temp_namespace, "pg_prepare", schema);
    if (table_oid == catalog::INVALID_TABLE_OID) {
      // Catalog wasn't able to proceed, txn must now abort
      return false;
    }

    // Instantiate a SqlTable and update the pointer in the Catalog
    auto *const table = new storage::SqlTable(node->GetBlockStore().Get(), schema);
    bool result UNUSED_ATTRIBUTE = accessor->SetTablePointer(table_oid, table);
  }

  // Insert a new Prepared Statement into the table
  auto table = accessor->GetTable(table_oid);
  std::vector<catalog::col_oid_t> table_cols;
  for (const auto &col : accessor->GetSchema(table_oid).GetColumns()) {
    table_cols.emplace_back(col.Oid());
  }
  auto pri = table->InitializerForProjectedRow(table_cols);
  const auto offsets = table->ProjectionMapForOids(table_cols);
  auto *const insert_redo = context->GetTxn()->StageWrite(context->DBOid(), table_oid, pri);
  auto *const insert_tuple = insert_redo->Delta();

  const auto varlen_entry = storage::StorageUtil::CreateVarlen(node->GetPreparedName());
  *reinterpret_cast<storage::VarlenEntry *>(insert_tuple->AccessForceNotNull(offsets.at(table_cols[0]))) = varlen_entry;
  *reinterpret_cast<int64_t *>(insert_tuple->AccessForceNotNull(offsets.at(table_cols[1]))) =
      reinterpret_cast<int64_t>(node.Get());
  insert_tuple->SetNull(offsets.at(table_cols[2]));
  insert_tuple->SetNull(offsets.at(table_cols[3]));

  accessor->GetTable(table_oid)->Insert(context->GetTxn(), insert_redo);
  return true;
}

bool DDLExecutors::CreateIndex(const common::ManagedPointer<exec::ExecutionContext> context,
                               const catalog::namespace_oid_t ns, const std::string &name,
                               const catalog::table_oid_t table, const catalog::IndexSchema &input_schema) {
  auto *const accessor = context->GetAccessor();
  // Request permission from the Catalog to see if this a valid namespace and table name
  const auto index_oid = accessor->CreateIndex(ns, table, name, input_schema);
  if (index_oid == catalog::INVALID_INDEX_OID) {
    // Catalog wasn't able to proceed, txn must now abort
    return false;
  }
  // Get the canonical IndexSchema from the Catalog now that column oids have been assigned
  const auto &schema = accessor->GetIndexSchema(index_oid);
  // Instantiate an Index and update the pointer in the Catalog
  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(schema);
  auto *const index = index_builder.Build();
  bool result UNUSED_ATTRIBUTE = accessor->SetIndexPointer(index_oid, index);
  TERRIER_ASSERT(result, "CreateIndex succeeded, SetIndexPointer must also succeed.");
  return true;
}
}  // namespace terrier::execution::sql
