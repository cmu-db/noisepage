#include "binder/bind_node_visitor.h"
#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "common/exception.h"
#include "common/managed_pointer.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/sql_statement.h"
#include "type/type_id.h"

namespace terrier::binder {

BindNodeVisitor::BindNodeVisitor(std::unique_ptr<catalog::CatalogAccessor> catalog_accessor,
                                 std::string default_database_name)
    : catalog_accessor_(std::move(catalog_accessor)), default_database_name_(std::move(default_database_name)) {}

void BindNodeVisitor::BindNameToNode(common::ManagedPointer<parser::SQLStatement> tree,
                                     parser::ParseResult *parse_result) {
  tree->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(parser::SelectStatement *node, parser::ParseResult *parse_result) {
  context_ = new BinderContext(context_);

  if (node->GetSelectTable() != nullptr) node->GetSelectTable()->Accept(this, parse_result);

  if (node->GetSelectCondition() != nullptr) {
    node->GetSelectCondition()->Accept(this, parse_result);
    node->GetSelectCondition()->DeriveDepth();
    node->GetSelectCondition()->DeriveSubqueryFlag();
  }
  if (node->GetSelectOrderBy() != nullptr) node->GetSelectOrderBy()->Accept(this, parse_result);

  if (node->GetSelectLimit() != nullptr) node->GetSelectLimit()->Accept(this, parse_result);

  if (node->GetSelectGroupBy() != nullptr) node->GetSelectGroupBy()->Accept(this, parse_result);

  std::vector<common::ManagedPointer<parser::AbstractExpression>> new_select_list;
  for (auto &select_element : node->GetSelectColumns()) {
    if (select_element->GetExpressionType() == parser::ExpressionType::STAR) {
      context_->GenerateAllColumnExpressions(parse_result, &new_select_list);
      continue;
    }

    select_element->Accept(this, parse_result);

    // Derive depth for all exprs in the select clause
    select_element->DeriveDepth();

    select_element->DeriveSubqueryFlag();

    // Traverse the expression to deduce expression value type and name
    select_element->DeriveReturnValueType();
    select_element->DeriveExpressionName();

    new_select_list.push_back(select_element);
  }
  node->SetSelectColumns(new_select_list);
  node->SetDepth(context_->GetDepth());

  auto curr_context = context_;
  context_ = context_->GetUpperContext();
  delete curr_context;
}

// Some sub query nodes inside SelectStatement
void BindNodeVisitor::Visit(parser::JoinDefinition *node, parser::ParseResult *parse_result) {
  // The columns in join condition can only bind to the join tables
  node->GetLeftTable()->Accept(this, parse_result);
  node->GetRightTable()->Accept(this, parse_result);
  node->GetJoinCondition()->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(parser::TableRef *node, parser::ParseResult *parse_result) {
  node->TryBindDatabaseName(default_database_name_);
  if (node->GetSelect() != nullptr) {
    if (node->GetAlias().empty()) throw BINDER_EXCEPTION("Alias not found for query derived table");

    // Save the previous context
    auto pre_context = context_;
    node->GetSelect()->Accept(this, parse_result);
    // Restore the previous level context
    context_ = pre_context;
    // Add the table to the current context at the end
    context_->AddNestedTable(node->GetAlias(), node->GetSelect()->GetSelectColumns());
  } else if (node->GetJoin() != nullptr) {
    // Join
    node->GetJoin()->Accept(this, parse_result);
  } else if (!node->GetList().empty()) {
    // Multiple table
    for (auto &table : node->GetList()) table->Accept(this, parse_result);
  } else {
    // Single table
    context_->AddRegularTable(catalog_accessor_, node);
  }
}

void BindNodeVisitor::Visit(parser::GroupByDescription *node, parser::ParseResult *parse_result) {
  for (auto &col : node->GetColumns()) col->Accept(this, parse_result);
  if (node->GetHaving() != nullptr) node->GetHaving()->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(parser::OrderByDescription *node, parser::ParseResult *parse_result) {
  for (auto &expr : node->GetOrderByExpressions())
    if (expr != nullptr) expr->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(parser::UpdateStatement *node, parser::ParseResult *parse_result) {
  context_ = new BinderContext(nullptr);

  node->GetUpdateTable()->Accept(this, parse_result);
  if (node->GetUpdateCondition() != nullptr) node->GetUpdateCondition()->Accept(this, parse_result);
  for (auto &update : node->GetUpdateClauses()) {
    // TODO(Ling): we need Accept() method for updateClause?
    update->GetUpdateValue()->Accept(this, parse_result);
  }

  // TODO(peloton): Update columns are not bound because they are char*
  //  not TupleValueExpression in update_statement.h
  delete context_;
  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::DeleteStatement *node, parser::ParseResult *parse_result) {
  context_ = new BinderContext(nullptr);
  node->GetDeletionTable()->TryBindDatabaseName(default_database_name_);
  auto table = node->GetDeletionTable();
  context_->AddRegularTable(catalog_accessor_, table->GetDatabaseName(), table->GetTableName(), table->GetTableName());

  if (node->GetDeleteCondition() != nullptr) {
    node->GetDeleteCondition()->Accept(this, parse_result);
  }

  delete context_;
  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::LimitDescription *node, parser::ParseResult *parse_result) {}

void BindNodeVisitor::Visit(parser::CopyStatement *node, parser::ParseResult *parse_result) {
  context_ = new BinderContext(nullptr);
  if (node->GetCopyTable() != nullptr) {
    node->GetCopyTable()->Accept(this, parse_result);

    // If the table is given, we're either writing or reading all columns
    std::vector<common::ManagedPointer<parser::AbstractExpression>> new_select_list;
    context_->GenerateAllColumnExpressions(parse_result, &new_select_list);
    auto columns = node->GetSelectStatement()->GetSelectColumns();
    columns.insert(std::end(columns), std::begin(new_select_list), std::end(new_select_list));
  } else {
    node->GetSelectStatement()->Accept(this, parse_result);
  }
}

void BindNodeVisitor::Visit(parser::CreateFunctionStatement *node, parser::ParseResult *parse_result) {}

void BindNodeVisitor::Visit(parser::CreateStatement *node, parser::ParseResult *parse_result) {
  node->TryBindDatabaseName(default_database_name_);
}

void BindNodeVisitor::Visit(parser::InsertStatement *node, parser::ParseResult *parse_result) {
  context_ = new BinderContext(nullptr);
  node->GetInsertionTable()->TryBindDatabaseName(default_database_name_);

  auto table = node->GetInsertionTable();
  context_->AddRegularTable(catalog_accessor_, table->GetDatabaseName(), table->GetTableName(), table->GetTableName());
  if (node->GetSelect() != nullptr) node->GetSelect()->Accept(this, parse_result);

  delete context_;
  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::DropStatement *node, parser::ParseResult *parse_result) {
  node->TryBindDatabaseName(default_database_name_);
}
void BindNodeVisitor::Visit(parser::PrepareStatement *node, parser::ParseResult *parse_result) {}
void BindNodeVisitor::Visit(parser::ExecuteStatement *node, parser::ParseResult *parse_result) {}
void BindNodeVisitor::Visit(parser::TransactionStatement *node, parser::ParseResult *parse_result) {}
void BindNodeVisitor::Visit(parser::AnalyzeStatement *node, parser::ParseResult *parse_result) {
  node->GetAnalyzeTable()->TryBindDatabaseName(default_database_name_);
}

void BindNodeVisitor::Visit(parser::ConstantValueExpression *expr, parser::ParseResult *parse_result) {}

void BindNodeVisitor::Visit(parser::ColumnValueExpression *expr, parser::ParseResult *parse_result) {
  // TODO(Ling): consider remove precondition check if the *_oid_ will never be initialized till binder
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
      if (context_ == nullptr || !context_->SetColumnPosTuple(expr)) {
        throw BINDER_EXCEPTION(("Cannot find column " + col_name).c_str());
      }
    } else {
      // Table name is present
      if (context_ != nullptr && context_->GetRegularTableObj(table_name, expr, &tuple)) {
        if (!BinderContext::ColumnInSchema(std::get<2>(tuple), col_name)) {
          throw BINDER_EXCEPTION(("Cannot find column " + col_name).c_str());
        }
        BinderContext::SetColumnPosTuple(col_name, tuple, expr);
      } else if (context_ == nullptr || !context_->CheckNestedTableColumn(table_name, col_name, expr)) {
        throw BINDER_EXCEPTION(("Invalid table reference " + expr->GetTableName()).c_str());
      }
    }
  }
}

void BindNodeVisitor::Visit(parser::CaseExpression *expr, parser::ParseResult *parse_result) {
  for (size_t i = 0; i < expr->GetWhenClauseSize(); ++i) {
    expr->GetWhenClauseCondition(i)->Accept(this, parse_result);
  }
}

void BindNodeVisitor::Visit(parser::SubqueryExpression *expr, parser::ParseResult *parse_result) {
  expr->GetSubselect()->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(parser::StarExpression *expr, parser::ParseResult *parse_result) {
  if (context_ == nullptr || !context_->HasTables()) {
    throw BINDER_EXCEPTION("Invalid [Expression :: STAR].");
  }
}

// Derive value type for these expressions
void BindNodeVisitor::Visit(parser::OperatorExpression *expr, parser::ParseResult *parse_result) {
  SqlNodeVisitor::Visit(expr, parse_result);
  expr->DeriveReturnValueType();
}
void BindNodeVisitor::Visit(parser::AggregateExpression *expr, parser::ParseResult *parse_result) {
  SqlNodeVisitor::Visit(expr, parse_result);
  expr->DeriveReturnValueType();
}

// void BindNodeVisitor::Visit(parser::FunctionExpression *expr, parser::ParseResult *parse_result) {
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
//  // TODO(Ling): Do we need GetFunction in catalog?
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
