#include "execution/sql/ddl_executors.h"

#include <memory>
#include <string>
#include <vector>
#include <stdio.h>
#include "catalog/catalog_accessor.h"
#include "catalog/postgres/pg_constraint.h"
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

namespace terrier::execution::sql {

bool DDLExecutors::CreateDatabaseExecutor(const common::ManagedPointer<planner::CreateDatabasePlanNode> node,
                                          const common::ManagedPointer<catalog::CatalogAccessor> accessor) {
  // Request permission from the Catalog to see if this a valid database name
  return accessor->CreateDatabase(node->GetDatabaseName()) != catalog::INVALID_DATABASE_OID;
}

bool DDLExecutors::CreateNamespaceExecutor(const common::ManagedPointer<planner::CreateNamespacePlanNode> node,
                                           const common::ManagedPointer<catalog::CatalogAccessor> accessor) {
  // Request permission from the Catalog to see if this a valid namespace name
  return accessor->CreateNamespace(node->GetNamespaceName()) != catalog::INVALID_NAMESPACE_OID;
}

bool DDLExecutors::CreateTableExecutor(const common::ManagedPointer<planner::CreateTablePlanNode> node,
                                       const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                       const catalog::db_oid_t connection_db) {
  // Request permission from the Catalog to see if this a valid namespace and table name
  const auto table_oid = accessor->CreateTable(node->GetNamespaceOid(), node->GetTableName(), *(node->GetSchema()));
  if (table_oid == catalog::INVALID_TABLE_OID) {
    // Catalog wasn't able to proceed, txn must now abort
    return false;
  }
  // Get the canonical Schema from the Catalog now that column oids have been assigned
  const auto &schema = accessor->GetSchema(table_oid);
  // Instantiate a SqlTable and update the pointer in the Catalog
  auto *const table = new storage::SqlTable(node->GetBlockStore(), schema);
  bool result = accessor->SetTablePointer(table_oid, table);
  TERRIER_ASSERT(result, "CreateTable succeeded, SetTablePointer must also succeed.");

  // if (node->HasPrimaryKey()) {
  //   // Create the IndexSchema Columns by referencing the Columns in the canonical table Schema from the Catalog
  //   std::vector<catalog::IndexSchema::Column> key_cols;
  //   const auto &primary_key_info = node->GetPrimaryKey();
  //   key_cols.reserve(primary_key_info.primary_key_cols_.size());
  //   for (const auto &parser_col : primary_key_info.primary_key_cols_) {
  //     const auto &table_col = schema.GetColumn(parser_col);
  //     if (table_col.Type() == type::TypeId::VARCHAR || table_col.Type() == type::TypeId::VARBINARY) {
  //       key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.MaxVarlenSize(), table_col.Nullable(),
  //                             parser::ColumnValueExpression(connection_db, table_oid, table_col.Oid()));

  //     } else {
  //       key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.Nullable(),
  //                             parser::ColumnValueExpression(connection_db, table_oid, table_col.Oid()));
  //     }
  //   }
  //   catalog::IndexSchema index_schema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);

  //   // Create the index, and use its return value as overall success result
  //   result = result &&
  //            CreateIndex(accessor, node->GetNamespaceOid(), primary_key_info.constraint_name_, table_oid, index_schema);
  // }

  // for (const auto &unique_constraint : node->GetUniqueConstraints()) {
  //   // TODO(Matt): we should add support in Catalog to update pg_constraint in this case
  //   // Create the IndexSchema Columns by referencing the Columns in the canonical table Schema from the Catalog
  //   std::vector<catalog::IndexSchema::Column> key_cols;
  //   for (const auto &unique_col : unique_constraint.unique_cols_) {
  //     const auto &table_col = schema.GetColumn(unique_col);
  //     if (table_col.Type() == type::TypeId::VARCHAR || table_col.Type() == type::TypeId::VARBINARY) {
  //       key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.MaxVarlenSize(), table_col.Nullable(),
  //                             parser::ColumnValueExpression(connection_db, table_oid, table_col.Oid()));

  //     } else {
  //       key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.Nullable(),
  //                             parser::ColumnValueExpression(connection_db, table_oid, table_col.Oid()));
  //     }
  //   }
  //   catalog::IndexSchema index_schema(key_cols, storage::index::IndexType::BWTREE, true, false, false, true);

  //   // Create the index, and use its return value as overall success result
  //   result = result && CreateIndex(accessor, node->GetNamespaceOid(), unique_constraint.constraint_name_, table_oid,
  //                                  index_schema);
  // }


  // create primary key indicies and its constraints
  result = result && DDLExecutors::CreatePKConstraintsAndIndices(accessor, schema, table_oid, node, connection_db);
  // create indicies and its constraints
  result = result && DDLExecutors::CreateUniqueConstraintsAndIndices(accessor, schema, table_oid, node, connection_db);
  // create foreign key indicies and its constraints
  result = result && DDLExecutors::CreateFKConstraintsAndIndices(accessor, schema, table_oid, node, connection_db);
  // create check indicies and its constraint
  result = result && DDLExecutors::CreateCheckConstraintsAndIndices(accessor, schema, table_oid, node, connection_db);
  // create exclusion indicies and its constraints
  result =
      result && DDLExecutors::CreateExclusionConstraintsAndIndices(accessor, schema, table_oid, node, connection_db);
  // create trigger constraints
  result = result && DDLExecutors::CreateTriggerConstraints(accessor, schema, table_oid, node, connection_db);
  // create NOTNULL constraints
  result = result && DDLExecutors::CreateNotNullConstraints(accessor, schema, table_oid, node, connection_db);

  return result;
}

bool DDLExecutors::CreateIndexExecutor(const common::ManagedPointer<planner::CreateIndexPlanNode> node,
                                       const common::ManagedPointer<catalog::CatalogAccessor> accessor) {
  return CreateIndex(accessor, node->GetNamespaceOid(), node->GetIndexName(), node->GetTableOid(),
                     *(node->GetSchema()));
}

bool DDLExecutors::DropDatabaseExecutor(const common::ManagedPointer<planner::DropDatabasePlanNode> node,
                                        const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                        const catalog::db_oid_t connection_db) {
  TERRIER_ASSERT(connection_db != node->GetDatabaseOid(),
                 "This command cannot be executed while connected to the target database. This should be checked in "
                 "the binder.");
  const bool result = accessor->DropDatabase(node->GetDatabaseOid());
  return result;
}

bool DDLExecutors::DropNamespaceExecutor(const common::ManagedPointer<planner::DropNamespacePlanNode> node,
                                         const common::ManagedPointer<catalog::CatalogAccessor> accessor) {
  const bool result = accessor->DropNamespace(node->GetNamespaceOid());
  // TODO(Matt): CASCADE?
  return result;
}

bool DDLExecutors::DropTableExecutor(const common::ManagedPointer<planner::DropTablePlanNode> node,
                                     const common::ManagedPointer<catalog::CatalogAccessor> accessor) {
  const bool result = accessor->DropTable(node->GetTableOid());
  // TODO(Matt): CASCADE?
  return result;
}

bool DDLExecutors::DropIndexExecutor(const common::ManagedPointer<planner::DropIndexPlanNode> node,
                                     const common::ManagedPointer<catalog::CatalogAccessor> accessor) {
  const bool result = accessor->DropIndex(node->GetIndexOid());
  // TODO(Matt): CASCADE?
  return result;
}

bool DDLExecutors::CreateIndex(const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                               const catalog::namespace_oid_t ns, const std::string &name,
                               const catalog::table_oid_t table, const catalog::IndexSchema &input_schema) {
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

catalog::index_oid_t DDLExecutors::CreateIndexForConstraints(
    const common::ManagedPointer<catalog::CatalogAccessor> accessor, const catalog::namespace_oid_t ns,
    const std::string &name, const catalog::table_oid_t table, const catalog::IndexSchema &input_schema) {
  // Request permission from the Catalog to see if this a valid namespace and table name
  const auto index_oid = accessor->CreateIndex(ns, table, name, input_schema);
  if (index_oid == catalog::INVALID_INDEX_OID) {
    // Catalog wasn't able to proceed, txn must now abort
    return catalog::INVALID_INDEX_OID;
  }
  // Get the canonical IndexSchema from the Catalog now that column oids have been assigned
  const auto &schema = accessor->GetIndexSchema(index_oid);
  // Instantiate an Index and update the pointer in the Catalog
  storage::index::IndexBuilder index_builder;
  index_builder.SetKeySchema(schema);
  auto *const index = index_builder.Build();
  bool result UNUSED_ATTRIBUTE = accessor->SetIndexPointer(index_oid, index);
  TERRIER_ASSERT(result, "CreateIndex succeeded, SetIndexPointer must also succeed.");
  return index_oid;
}

bool DDLExecutors::CreatePKConstraintsAndIndices(const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                                 const terrier::catalog::Schema &schema,
                                                 const catalog::table_oid_t table,
                                                 const common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                                 const catalog::db_oid_t connection_db) {
  catalog::constraint_oid_t constraint_oid;
  catalog::namespace_oid_t ns = plan_node->GetNamespaceOid();
  if (plan_node->HasPrimaryKey()) {
    std::cerr << "Enter create PK and Index\n";
    // Create the IndexSchema Columns by referencing the Columns in the canonical table Schema from the Catalog
    std::vector<catalog::IndexSchema::Column> key_cols;
    const auto &primary_key_info = plan_node->GetPrimaryKey();
    key_cols.reserve(primary_key_info.primary_key_cols_.size());
    for (const auto &parser_col : primary_key_info.primary_key_cols_) {
      const auto &table_col = schema.GetColumn(parser_col);
      if (table_col.Type() == type::TypeId::VARCHAR || table_col.Type() == type::TypeId::VARBINARY) {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.MaxVarlenSize(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, table, table_col.Oid()));

      } else {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, table, table_col.Oid()));
      }
    }
    catalog::IndexSchema index_schema(key_cols, storage::index::IndexType::BWTREE, true, true, false, true);
    

    // Create the index, and use its return value as overall success result
    catalog::index_oid_t index_oid = CreateIndexForConstraints(accessor, plan_node->GetNamespaceOid(),
                                                               primary_key_info.constraint_name_, table, index_schema);
    std::cerr << "created index\n";
    if (index_oid == catalog::INVALID_INDEX_OID) {
      std::cerr << "created index for constraint failed\n";
      return false;
    }
    const planner::PrimaryKeyInfo &pk_info = plan_node->GetPrimaryKey();
    std::vector<catalog::col_oid_t> pk_cols;
    pk_cols.reserve(pk_info.primary_key_cols_.size());
    for (const auto &col_name : pk_info.primary_key_cols_) {
      pk_cols.push_back(schema.GetColumn(col_name).Oid());
    }
    constraint_oid = accessor->CreatePKConstraint(ns, table, pk_info.constraint_name_, index_oid, pk_cols);
    if (constraint_oid == catalog::INVALID_CONSTRAINT_OID) {
       std::cerr << "Catalog wasn't able to proceed, txn must now abort\n";
      return false;
    }
  }
  return true;
}
bool DDLExecutors::CreateFKConstraintsAndIndices(const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                                 const terrier::catalog::Schema &schema,
                                                 const catalog::table_oid_t table,
                                                 const common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                                 const catalog::db_oid_t connection_db) {
  catalog::constraint_oid_t constraint_oid;
  catalog::namespace_oid_t ns = plan_node->GetNamespaceOid();
  // try create all FK constraints
  const std::vector<planner::ForeignKeyInfo> fks = plan_node->GetForeignKeys();
  std::vector<catalog::col_oid_t> src_cols;
  std::vector<catalog::col_oid_t> sink_cols;
  catalog::table_oid_t src_table;
  catalog::table_oid_t sink_table;
  src_table = table;
  for (const auto &fk : fks) {
    src_cols.clear();
    sink_cols.clear();
    sink_table = accessor->GetTableOid(fk.sink_table_name_);
    const terrier::catalog::Schema &sink_schema = accessor->GetSchema(sink_table);
    src_cols.reserve(fk.foreign_key_sources_.size());
    sink_cols.reserve(fk.foreign_key_sinks_.size());
    // create a index for the foreign key sink table col combinations
    std::vector<catalog::IndexSchema::Column> key_cols;
    for (const auto &fk_col : fk.foreign_key_sinks_) {
      const auto &table_col = sink_schema.GetColumn(fk_col);
      if (table_col.Type() == type::TypeId::VARCHAR || table_col.Type() == type::TypeId::VARBINARY) {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.MaxVarlenSize(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, sink_table, table_col.Oid()));

      } else {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, sink_table, table_col.Oid()));
      }
    }
    catalog::IndexSchema index_schema(key_cols, storage::index::IndexType::BWTREE, true, false, false, true);
    // Create the index, and use its return value as overall success result
    catalog::index_oid_t index_oid = CreateIndexForConstraints(accessor, plan_node->GetNamespaceOid(),
                                                               fk.constraint_name_, sink_table, index_schema);
    if (index_oid == catalog::INVALID_INDEX_OID) {
      return false;
    }

    for (const auto &col_name : fk.foreign_key_sources_) {
      src_cols.push_back(schema.GetColumn(col_name).Oid());
    }
    for (const auto &col_name : fk.foreign_key_sinks_) {
      sink_cols.push_back(sink_schema.GetColumn(col_name).Oid());
    }
    std::unordered_map<terrier::parser::FKConstrActionType, catalog::postgres::FKActionType>
        fk_constructuin_action_map = {
            {terrier::parser::FKConstrActionType::INVALID, catalog::postgres::FKActionType::SETINVALID},
            {terrier::parser::FKConstrActionType::NOACTION, catalog::postgres::FKActionType::NOACT},
            {terrier::parser::FKConstrActionType::RESTRICT_, catalog::postgres::FKActionType::RESTRICTION},
            {terrier::parser::FKConstrActionType::CASCADE, catalog::postgres::FKActionType::CASCADE},
            {terrier::parser::FKConstrActionType::SETNULL, catalog::postgres::FKActionType::SETNULL},
            {terrier::parser::FKConstrActionType::SETDEFAULT, catalog::postgres::FKActionType::SETDEFAULT}};
    catalog::postgres::FKActionType update_action = fk_constructuin_action_map[fk.upd_action_];
    catalog::postgres::FKActionType delete_action = fk_constructuin_action_map[fk.del_action_];
    constraint_oid = accessor->CreateFKConstraints(ns, src_table, sink_table, fk.constraint_name_, index_oid, src_cols,
                                                   sink_cols, update_action, delete_action);
    if (constraint_oid == catalog::INVALID_CONSTRAINT_OID) {
      // Catalog wasn't able to proceed, txn must now abort
      return false;
    }
  }
  return true;
}

bool DDLExecutors::CreateUniqueConstraintsAndIndices(
    const common::ManagedPointer<catalog::CatalogAccessor> accessor, const terrier::catalog::Schema &schema,
    const catalog::table_oid_t table, const common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
    const catalog::db_oid_t connection_db) {
  catalog::constraint_oid_t constraint_oid;
  catalog::namespace_oid_t ns = plan_node->GetNamespaceOid();
  std::vector<catalog::col_oid_t> unique_cols;
  for (const auto &unique_constraint : plan_node->GetUniqueConstraints()) {
    // TODO(Matt): we should add support in Catalog to update pg_constraint in this case
    // Create the IndexSchema Columns by referencing the Columns in the canonical table Schema from the Catalog
    std::vector<catalog::IndexSchema::Column> key_cols;
    for (const auto &unique_col : unique_constraint.unique_cols_) {
      const auto &table_col = schema.GetColumn(unique_col);
      if (table_col.Type() == type::TypeId::VARCHAR || table_col.Type() == type::TypeId::VARBINARY) {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.MaxVarlenSize(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, table, table_col.Oid()));

      } else {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, table, table_col.Oid()));
      }
    }
    catalog::IndexSchema index_schema(key_cols, storage::index::IndexType::BWTREE, true, false, false, true);

    // Create the index, and use its return value as overall success result
    catalog::index_oid_t index_oid = CreateIndexForConstraints(accessor, plan_node->GetNamespaceOid(),
                                                               unique_constraint.constraint_name_, table, index_schema);
    if (index_oid == catalog::INVALID_INDEX_OID) {
      return false;
    }
    unique_cols.clear();
    unique_cols.reserve(unique_constraint.unique_cols_.size());
    for (const auto &col_name : unique_constraint.unique_cols_) {
      unique_cols.push_back(schema.GetColumn(col_name).Oid());
    }
    constraint_oid =
        accessor->CreateUNIQUEConstraints(ns, table, unique_constraint.constraint_name_, index_oid, unique_cols);
    if (constraint_oid == catalog::INVALID_CONSTRAINT_OID) {
      // Catalog wasn't able to proceed, txn must now abort
      return false;
    }
  }
  return true;
}
bool DDLExecutors::CreateCheckConstraintsAndIndices(
    const common::ManagedPointer<catalog::CatalogAccessor> accessor, const terrier::catalog::Schema &schema,
    const catalog::table_oid_t table, const common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
    const catalog::db_oid_t connection_db) {
  // catalog::constraint_oid_t constraint_oid;
  catalog::namespace_oid_t ns = plan_node->GetNamespaceOid();
  return (ns != catalog::INVALID_NAMESPACE_OID);
}
bool DDLExecutors::CreateExclusionConstraintsAndIndices(
    const common::ManagedPointer<catalog::CatalogAccessor> accessor, const terrier::catalog::Schema &schema,
    const catalog::table_oid_t table, const common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
    const catalog::db_oid_t connection_db) {
  // catalog::constraint_oid_t constraint_oid;
  catalog::namespace_oid_t ns = plan_node->GetNamespaceOid();
  return (ns != catalog::INVALID_NAMESPACE_OID);
}
bool DDLExecutors::CreateNotNullConstraints(const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                            const terrier::catalog::Schema &schema, const catalog::table_oid_t table,
                                            const common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                            const catalog::db_oid_t connection_db) {
  // catalog::constraint_oid_t constraint_oid;
  catalog::namespace_oid_t ns = plan_node->GetNamespaceOid();
  return (ns != catalog::INVALID_NAMESPACE_OID);
}
bool DDLExecutors::CreateTriggerConstraints(const common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                            const terrier::catalog::Schema &schema, const catalog::table_oid_t table,
                                            const common::ManagedPointer<planner::CreateTablePlanNode> plan_node,
                                            const catalog::db_oid_t connection_db) {
  // catalog::constraint_oid_t constraint_oid;
  catalog::namespace_oid_t ns = plan_node->GetNamespaceOid();
  return (ns != catalog::INVALID_NAMESPACE_OID);
}

}  // namespace terrier::execution::sql
