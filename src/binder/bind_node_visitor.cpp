#include "binder/bind_node_visitor.h"
#include "common/exception.h"
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "type/type_id.h"
#include "parser/sql_statement.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/column_value_expression.h"

namespace terrier::binder {

BindNodeVisitor::BindNodeVisitor(catalog::CatalogAccessor* catalog_accessor, std::string default_database_name)
    : catalog_accessor_(catalog_accessor), default_database_name_(std::move(default_database_name)) {}

void BindNodeVisitor::BindNameToNode(parser::SQLStatement *tree) {
  tree->Accept(this);
}

void BindNodeVisitor::Visit(parser::SelectStatement *node) {
  // TODO: remove make shared ... Use raw pointers, as the context is not stored in the binder
  //  Statement will not be modified
  //  Expression will be modified but only alias and tupleValueExpression
  //  We can make binder friend of abstract expression
  context_ = std::make_shared<BinderContext>(context_);

  if (node->GetSelectTable() != nullptr) node->GetSelectTable()->Accept(this);

  if (node->GetSelectCondition() != nullptr) {
    node->GetSelectCondition()->Accept(this);
    node->GetSelectCondition()->DeriveDepth();
    node->GetSelectCondition()->DeriveSubqueryFlag();
  }
  if (node->GetSelectOrderBy() != nullptr) node->GetSelectOrderBy()->Accept(this);

  if (node->GetSelectLimit() != nullptr) node->GetSelectLimit()->Accept(this);

  if (node->GetSelectGroupBy() != nullptr) node->GetSelectGroupBy()->Accept(this);

  // TODO (Ling): change the it to managed_ptr
  std::vector<std::shared_ptr<parser::AbstractExpression>> new_select_list;
  for (auto &select_element : node->GetSelectColumns()) {
    if (select_element->GetExpressionType() == parser::ExpressionType::STAR) {
      context_->GenerateAllColumnExpressions(new_select_list);
      continue;
    }

    select_element->Accept(this);

    // Derive depth for all exprs in the select clause
    if (select_element->GetExpressionType() == parser::ExpressionType::STAR) select_element->SetDepth(context_->GetDepth());
    else select_element->DeriveDepth();

    select_element->DeriveSubqueryFlag();

    // Traverse the expression to deduce expression value type and name
    select_element->DeriveReturnValueType();
    select_element->DeriveExpressionName();

    new_select_list.push_back(std::move(select_element));
  }
  node->SetSelectColumns(new_select_list);
  node->SetDepth(context_->GetDepth());
  context_ = context_->GetUpperContext();
}

// Some sub query nodes inside SelectStatement
void BindNodeVisitor::Visit(parser::JoinDefinition *node) {
  // The columns in join condition can only bind to the join tables
  node->GetLeftTable()->Accept(this);
  node->GetRightTable()->Accept(this);
  node->GetJoinCondition()->Accept(this);
}

void BindNodeVisitor::Visit(parser::TableRef *node) {
  node->TryBindDatabaseName(default_database_name_);
  if (node->GetSelect() != nullptr) {
    if (node->GetAlias().empty()) throw BINDER_EXCEPTION("Alias not found for query derived table");

    // Save the previous context
    auto pre_context = context_;
    node->GetSelect()->Accept(this);
    // Restore the previous level context
    context_ = pre_context;
    // Add the table to the current context at the end
    context_->AddNestedTable(node->GetAlias(), node->GetSelect()->GetSelectColumns());
  }

  // Join
  else if (node->GetJoin() != nullptr) node->GetJoin()->Accept(this);

  // Multiple table
  else if (!node->GetList().empty()) for (auto &table : node->GetList()) table->Accept(this);

  // Single table
  else context_->AddRegularTable(catalog_accessor_, node);
}

void BindNodeVisitor::Visit(parser::GroupByDescription *node) {
  for (auto &col : node->GetColumns()) col->Accept(this);
  if (node->GetHaving() != nullptr) node->GetHaving()->Accept(this);
}

void BindNodeVisitor::Visit(parser::OrderByDescription *node) {
  for (auto &expr : node->GetOrderByExpressions())
    if (expr != nullptr) expr->Accept(this);
}

void BindNodeVisitor::Visit(parser::UpdateStatement *node) {
  context_ = std::make_shared<BinderContext>(nullptr);

  node->GetUpdateTable()->Accept(this);
  if (node->GetUpdateCondition() != nullptr) node->GetUpdateCondition()->Accept(this);
  for (auto &update : node->GetUpdateClauses()) {
    // TODO: we need Accept() method for updateClause?
    update->GetUpdateValue()->Accept(this);
  }

  // TODO(peloton): Update columns are not bound because they are char*
  //  not TupleValueExpression in update_statement.h

  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::DeleteStatement *node) {
  context_ = std::make_shared<BinderContext>(nullptr);
  // no try bind anything
  node->GetDeletionTable()->TryBindDatabaseName(default_database_name_);
  auto table = node->GetDeletionTable();
  context_->AddRegularTable(catalog_accessor_, table->GetDatabaseName(), table->GetTableName(), table->GetTableName());

  if (node->GetDeleteCondition() != nullptr) {
    node->GetDeleteCondition()->Accept(this);
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::LimitDescription *) {}

void BindNodeVisitor::Visit(parser::CopyStatement *node) {
  // TODO (Ling): new binder context?
  context_ = std::make_shared<BinderContext>(nullptr);
  if (node->GetCopyTable() != nullptr) {
    node->GetCopyTable()->Accept(this);

    // If the table is given, we're either writing or reading all columns
    std::vector<std::shared_ptr<parser::AbstractExpression>> new_select_list;
    context_->GenerateAllColumnExpressions(new_select_list);
    auto columns = node->GetSelectStatement()->GetSelectColumns();
    columns.insert(std::end(columns), std::begin(new_select_list), std::end(new_select_list));
  } else {
    node->GetSelectStatement()->Accept(this);
  }
}

void BindNodeVisitor::Visit(parser::CreateFunctionStatement *) {}

void BindNodeVisitor::Visit(parser::CreateStatement *node) {
  // TODO: try nothing.
  //  Try  bind database name will set, for the table_ref of the node,
  //  the db_name and schema_name to default if they are not there
  // TODO: new: go to the node and see if it has db name; rename the function to fit its purpose
  //  if node->db_name is empty:
  //    set it to defualt
  //  else if node->db_name != defualt db_name:
  //    throw exception
  //  return;
  node->TryBindDatabaseName(default_database_name_);
}

void BindNodeVisitor::Visit(parser::InsertStatement *node) {
  node->GetInsertionTable()->TryBindDatabaseName(default_database_name_);
  context_ = std::make_shared<BinderContext>(nullptr);

  auto table = node->GetInsertionTable();
  context_->AddRegularTable(catalog_accessor_, table->GetDatabaseName(), table->GetTableName(), table->GetTableName());
  if (node->GetSelect() != nullptr) node->GetSelect()->Accept(this);

  context_ = nullptr;
}
void BindNodeVisitor::Visit(parser::DropStatement *node) { node->TryBindDatabaseName(default_database_name_); }
void BindNodeVisitor::Visit(parser::PrepareStatement *) {}
void BindNodeVisitor::Visit(parser::ExecuteStatement *) {}
void BindNodeVisitor::Visit(parser::TransactionStatement *) {}
void BindNodeVisitor::Visit(parser::AnalyzeStatement *node) { node->GetAnalyzeTable()->TryBindDatabaseName(default_database_name_); }

void BindNodeVisitor::Visit(parser::ConstantValueExpression *) {}

void BindNodeVisitor::Visit(parser::ColumnValueExpression *expr) {

  // TODO (Ling): consider remove precondition check if the *_oid_ will never be initialized till binder
  //   That is, the object would not be initialized using ColumnValueeExpression(database_oid, table_oid, column_oid)
  //   at this point
  if (expr->GetTableOid() == catalog::INVALID_TABLE_OID) {

    std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::Schema> tuple;
    std::string table_name = expr->GetTableName();
    std::string col_name = expr->GetColumnName();

    // Convert all the names to lower cases
    std::transform(table_name.begin(), table_name.end(), table_name.begin(), ::tolower);
    std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::tolower);

    // Table name not specified in the expression. Loop through all the table in the binder context.
    if (table_name.empty()) {
      if (!BinderContext::GetColumnPosTuple(context_, expr)) {
        throw BINDER_EXCEPTION(("Cannot find column " + col_name).c_str());
      }
    }
      // Table name is present
    else {
      // Regular table
      if (BinderContext::GetRegularTableObj(context_, table_name, expr, tuple)) {
        if (!BinderContext::ColumnInSchema(std::get<2>(tuple), col_name)) {
          throw BINDER_EXCEPTION(("Cannot find column " + col_name).c_str());
        }
        BinderContext::GetColumnPosTuple(col_name, tuple, expr);
      }
        // Nested table
      else if (!BinderContext::CheckNestedTableColumn(context_, table_name, col_name, expr))
        throw BINDER_EXCEPTION(("Invalid table reference " + expr->GetTableName()).c_str());
    }
  }
}

void BindNodeVisitor::Visit(parser::CaseExpression *expr) {
  for (size_t i = 0; i < expr->GetWhenClauseSize(); ++i) {
    expr->GetWhenClauseCondition(i)->Accept(this);
  }
}

void BindNodeVisitor::Visit(parser::SubqueryExpression *expr) {
  expr->GetSubselect()->Accept(this);
}

void BindNodeVisitor::Visit(parser::StarExpression *expr) {
  if (!BinderContext::HasTables(context_)) {
    throw BINDER_EXCEPTION("Invalid [Expression :: STAR].");
  }
}

// Derive value type for these expressions
void BindNodeVisitor::Visit(parser::OperatorExpression *expr) {
  SqlNodeVisitor::Visit(expr);
  expr->DeriveReturnValueType();
}
void BindNodeVisitor::Visit(parser::AggregateExpression *expr) {
  SqlNodeVisitor::Visit(expr);
  expr->DeriveReturnValueType();
}

//void BindNodeVisitor::Visit(parser::FunctionExpression *expr) {
//  // Visit the subtree first
//  SqlNodeVisitor::Visit(expr);
//
//  // Check catalog and bind function
//  std::vector<type::TypeId> argtypes;
//  for (size_t i = 0; i < expr->GetChildrenSize(); i++)
//    argtypes.push_back(expr->GetChild(i)->GetReturnValueType());
//  // Check and set the function ptr
//  // TODO(boweic): Visit the catalog using the interface that is protected by
//  // transaction
//  // TODO (Ling): Do we need GetFunction in catalog?
//  const catalog::FunctionData &func_data =
//      catalog_accessor_->GetFunction(expr->GetFuncName(), argtypes);
//  LOG_DEBUG("Function %s found in the catalog", func_data.func_name_.c_str());
//  LOG_DEBUG("Argument num: %ld", func_data.argument_types_.size());
//  LOG_DEBUG("Is UDF %d", func_data.is_udf_);
//
//  if (!func_data.is_udf_) {
//    expr->SetBuiltinFunctionExpressionParameters(
//        func_data.func_, func_data.return_type_, func_data.argument_types_);
//
//    // Look into the OperatorId for built-in functions to check the first
//    // argument for timestamp functions.
//    // TODO(LM): The OperatorId might need to be changed to global ID after we
//    // rewrite the function identification logic.
//    auto func_operator_id = func_data.func_.op_id;
//    if (func_operator_id == OperatorId::DateTrunc ||
//        func_operator_id == OperatorId::DatePart) {
//      auto date_part = expr->GetChild(0);
//
//      // Test whether the first argument is a correct DatePartType
//      StringToDatePartType(
//          date_part->Evaluate(nullptr, nullptr, nullptr).ToString());
//    }
//  } else {
//    expr->SetUDFFunctionExpressionParameters(func_data.func_context_,
//                                             func_data.return_type_,
//                                             func_data.argument_types_);
//  }
//}

}  // namespace terrier::binder
