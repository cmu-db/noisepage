#pragma once

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

namespace terrier::execution {

class DDLUtil {
 public:
  DDLUtil() = delete;

  static bool CreateTableExecutor(const common::ManagedPointer<planner::CreateTablePlanNode> node,
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
          key_cols.emplace_back(
              table_col.Name(), table_col.Type(), table_col.MaxVarlenSize(), table_col.Nullable(),
              parser::ColumnValueExpression(context->DBOid(), table_oid,
                                            table_col.Oid()));  // TODO(Matt): is table_col.Name() right?

        } else {
          key_cols.emplace_back(
              table_col.Name(), table_col.Type(), table_col.Nullable(),
              parser::ColumnValueExpression(context->DBOid(), table_oid,
                                            table_col.Oid()));  // TODO(Matt): is table_col.Name() right?
        }
      }
      auto index_schema =
          std::make_unique<catalog::IndexSchema>(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);

      planner::CreateIndexPlanNode::Builder builder;
      builder.SetDatabaseOid(context->DBOid())
          .SetNamespaceOid(node->GetNamespaceOid())
          .SetTableOid(table_oid)
          .SetIndexName(primary_key_info.constraint_name_)
          .SetSchema(std::move(index_schema));

      auto create_index_node = builder.Build();

      return CreateIndexExecutor(common::ManagedPointer<planner::CreateIndexPlanNode>(create_index_node), context);
    }

    return true;
  }

  static bool CreateIndexExecutor(const common::ManagedPointer<planner::CreateIndexPlanNode> node,
                                  const common::ManagedPointer<exec::ExecutionContext> context) {
    auto *const accessor = context->GetAccessor();
    // Request permission from the Catalog to see if this a valid namespace and table name
    const auto index_oid =
        accessor->CreateIndex(node->GetNamespaceOid(), node->GetTableOid(), node->GetIndexName(), *(node->GetSchema()));
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

  static bool CreateDatabaseExecutor(const common::ManagedPointer<planner::CreateDatabasePlanNode> node,
                                     const common::ManagedPointer<exec::ExecutionContext> context) {
    auto *const accessor = context->GetAccessor();
    // Request permission from the Catalog to see if this a valid database name
    return accessor->CreateDatabase(node->GetDatabaseName()) != catalog::INVALID_DATABASE_OID;
  }

  static bool CreateNamespaceExecutor(const common::ManagedPointer<planner::CreateNamespacePlanNode> node,
                                      const common::ManagedPointer<exec::ExecutionContext> context) {
    auto *const accessor = context->GetAccessor();
    // Request permission from the Catalog to see if this a valid namespace name
    return accessor->CreateNamespace(node->GetNamespaceName()) != catalog::INVALID_NAMESPACE_OID;
  }

  static bool DropTableExecutor(const common::ManagedPointer<planner::DropTablePlanNode> node,
                                const common::ManagedPointer<exec::ExecutionContext> context) {
    auto *const accessor = context->GetAccessor();
    const bool result = accessor->DropTable(node->GetTableOid());
    return result;
  }

  static bool DropIndexExecutor(const common::ManagedPointer<planner::DropIndexPlanNode> node,
                                const common::ManagedPointer<exec::ExecutionContext> context) {
    auto *const accessor = context->GetAccessor();
    const bool result = accessor->DropIndex(node->GetIndexOid());
    return result;
  }

  static bool DropDatabaseExecutor(const common::ManagedPointer<planner::DropDatabasePlanNode> node,
                                   const common::ManagedPointer<exec::ExecutionContext> context) {
    TERRIER_ASSERT(context->DBOid() != node->GetDatabaseOid(),
                   "This command cannot be executed while connected to the target database. This should be checked in "
                   "the binder.");
    auto *const accessor = context->GetAccessor();
    const bool result = accessor->DropDatabase(node->GetDatabaseOid());
    return result;
  }

  static bool DropNamespaceExecutor(const common::ManagedPointer<planner::DropNamespacePlanNode> node,
                                    const common::ManagedPointer<exec::ExecutionContext> context) {
    auto *const accessor = context->GetAccessor();
    const bool result = accessor->DropNamespace(node->GetNamespaceOid());
    return result;
  }
};
}  // namespace terrier::execution
