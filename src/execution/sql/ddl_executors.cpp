#include "execution/sql/ddl_executors.h"

#include <memory>
#include <string>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/postgres/pg_language.h"
#include "common/macros.h"
#include "execution/ast/ast_pretty_print.h"
#include "execution/ast/context.h"
#include "execution/ast/udf/udf_ast_context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/udf/udf_codegen.h"
#include "execution/exec/execution_context.h"
#include "execution/sema/sema.h"
#include "loggers/execution_logger.h"
#include "parser/expression/column_value_expression.h"
#include "parser/udf/plpgsql_parser.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_function_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "storage/index/index_builder.h"
#include "storage/sql_table.h"

namespace noisepage::execution::sql {

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

bool DDLExecutors::CreateFunctionExecutor(const common::ManagedPointer<planner::CreateFunctionPlanNode> node,
                                          const common::ManagedPointer<catalog::CatalogAccessor> accessor) {
  // Request permission from the Catalog to see if this a valid namespace name
  NOISEPAGE_ASSERT(node->GetUDFLanguage() == parser::PLType::PL_PGSQL, "Unsupported language");
  NOISEPAGE_ASSERT(!node->GetFunctionBody().empty(), "Unsupported function body contents");

  // I don't like how we have to separate the two here
  std::vector<type::TypeId> param_type_ids{};
  std::vector<catalog::type_oid_t> param_types{};
  for (const auto t : node->GetFunctionParameterTypes()) {
    param_type_ids.push_back(parser::FuncParameter::DataTypeToTypeId(t));
    param_types.push_back(accessor->GetTypeOidFromTypeId(parser::FuncParameter::DataTypeToTypeId(t)));
  }

  auto body = node->GetFunctionBody().front();
  auto proc_id = accessor->CreateProcedure(
      node->GetFunctionName(), catalog::postgres::PgLanguage::PLPGSQL_LANGUAGE_OID, node->GetNamespaceOid(),
      node->GetFunctionParameterNames(), param_types, param_types, {},
      accessor->GetTypeOidFromTypeId(parser::ReturnType::DataTypeToTypeId(node->GetReturnType())), body, false);
  if (proc_id == catalog::INVALID_PROC_OID) {
    return false;
  }

  // Make the context here using the body
  ast::udf::UdfAstContext udf_ast_context{};

  // TODO(Kyle): Revisit this after clearing up what the
  // preferred way to report errors is in the system, both
  // within components and between components...
  parser::udf::PLpgSQLParser udf_parser{common::ManagedPointer{&udf_ast_context}, accessor, node->GetDatabaseOid()};
  std::unique_ptr<ast::udf::FunctionAST> ast{};
  try {
    ast = udf_parser.Parse(node->GetFunctionParameterNames(), param_type_ids, body);
  } catch (Exception &e) {
    return false;
  }

  // TODO(Kyle): Is this leaked?
  auto region = new util::Region(node->GetFunctionName());
  sema::ErrorReporter error_reporter{region};

  auto ast_context = std::make_unique<ast::Context>(region, &error_reporter);

  compiler::CodeGen codegen{ast_context.get(), accessor.Get()};
  util::RegionVector<ast::FieldDecl *> fn_params{codegen.GetAstContext()->GetRegion()};
  fn_params.emplace_back(
      codegen.MakeField(codegen.MakeFreshIdentifier("executionCtx"),
                        codegen.PointerType(codegen.BuiltinType(ast::BuiltinType::ExecutionContext))));

  for (auto i = 0UL; i < node->GetFunctionParameterNames().size(); i++) {
    const auto name = node->GetFunctionParameterNames()[i];
    const auto type = parser::ReturnType::DataTypeToTypeId(node->GetFunctionParameterTypes()[i]);
    fn_params.emplace_back(
        codegen.MakeField(ast_context->GetIdentifier(name), codegen.TplType(execution::sql::GetTypeId(type))));
  }

  auto name = node->GetFunctionName();
  compiler::FunctionBuilder fb{
      &codegen, codegen.MakeFreshIdentifier(name), std::move(fn_params),
      codegen.TplType(execution::sql::GetTypeId(parser::ReturnType::DataTypeToTypeId(node->GetReturnType())))};

  // Run UDF code generation
  auto *file = compiler::udf::UdfCodegen::Run(accessor.Get(), &fb, &udf_ast_context, &codegen, node->GetDatabaseOid(),
                                              ast.get());

  {
    sema::Sema type_check{codegen.GetAstContext().Get()};
    type_check.GetErrorReporter()->Reset();
    if (type_check.Run(file)) {
      EXECUTION_LOG_ERROR("Errors: \n {}", type_check.GetErrorReporter()->SerializeErrors());
      return false;
    }
  }

  // TODO(Kyle): We are recomputing the types here because we lost
  // them to a std::move() above when we generate the AST, can we
  // avoid duplicating this work? Would need to change the APIS.

  std::vector<type::TypeId> types{};
  types.reserve(node->GetFunctionParameterTypes().size());
  std::transform(node->GetFunctionParameterTypes().cbegin(), node->GetFunctionParameterTypes().cend(),
                 std::back_inserter(types), [](const parser::BaseFunctionParameter::DataType &type) -> type::TypeId {
                   return parser::FuncParameter::DataTypeToTypeId(type);
                 });

  auto udf_context = std::make_unique<functions::FunctionContext>(
      node->GetFunctionName(), parser::ReturnType::DataTypeToTypeId(node->GetReturnType()), std::move(types),
      std::unique_ptr<util::Region>(region), std::move(ast_context), file);
  if (!accessor->SetFunctionContextPointer(proc_id, udf_context.get())) {
    return false;
  }

  // TODO(Kyle): Not quite sure how abort actions work, but is
  // the implication here that we leak in the event that we do
  // not abort and the associated transaction completes?
  accessor->GetTxn()->RegisterAbortAction([udf_context = udf_context.release()]() { delete udf_context; });
  return true;
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
  NOISEPAGE_ASSERT(result, "CreateTable succeeded, SetTablePointer must also succeed.");

  if (node->HasPrimaryKey()) {
    // Create the IndexSchema Columns by referencing the Columns in the canonical table Schema from the Catalog
    std::vector<catalog::IndexSchema::Column> key_cols;
    const auto &primary_key_info = node->GetPrimaryKey();
    key_cols.reserve(primary_key_info.primary_key_cols_.size());
    for (const auto &parser_col : primary_key_info.primary_key_cols_) {
      const auto &table_col = schema.GetColumn(parser_col);
      if (table_col.Type() == type::TypeId::VARCHAR || table_col.Type() == type::TypeId::VARBINARY) {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.TypeModifier(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, table_oid, table_col.Oid()));

      } else {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, table_oid, table_col.Oid()));
      }
    }
    catalog::IndexOptions options;
    catalog::IndexSchema index_schema(key_cols, storage::index::IndexType::BPLUSTREE, true, true, false, true, options);

    // Create the index, and use its return value as overall success result
    result = result &&
             CreateIndex(accessor, node->GetNamespaceOid(), primary_key_info.constraint_name_, table_oid, index_schema);
  }

  for (const auto &unique_constraint : node->GetUniqueConstraints()) {
    // TODO(Matt): we should add support in Catalog to update pg_constraint in this case
    // Create the IndexSchema Columns by referencing the Columns in the canonical table Schema from the Catalog
    std::vector<catalog::IndexSchema::Column> key_cols;
    for (const auto &unique_col : unique_constraint.unique_cols_) {
      const auto &table_col = schema.GetColumn(unique_col);
      if (table_col.Type() == type::TypeId::VARCHAR || table_col.Type() == type::TypeId::VARBINARY) {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.TypeModifier(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, table_oid, table_col.Oid()));

      } else {
        key_cols.emplace_back(table_col.Name(), table_col.Type(), table_col.Nullable(),
                              parser::ColumnValueExpression(connection_db, table_oid, table_col.Oid()));
      }
    }
    catalog::IndexOptions options;
    catalog::IndexSchema index_schema(key_cols, storage::index::IndexType::BPLUSTREE, true, false, false, true,
                                      options);

    // Create the index, and use its return value as overall success result
    result = result && CreateIndex(accessor, node->GetNamespaceOid(), unique_constraint.constraint_name_, table_oid,
                                   index_schema);
  }

  // TODO(Matt): interpret other fields in CreateTablePlanNode when we support them in the Catalog:
  // foreign_keys_, con_checks_,

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
  NOISEPAGE_ASSERT(connection_db != node->GetDatabaseOid(),
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
  NOISEPAGE_ASSERT(result, "CreateIndex succeeded, SetIndexPointer must also succeed.");
  return true;
}

}  // namespace noisepage::execution::sql
