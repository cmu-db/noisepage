#include "binder/bind_node_visitor.h"
#include "common/exception.h"
#include "catalog/catalog_accessor.h"
#include "parser/expression/expression_util.h"
#include "type/type_id.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/tuple_value_expression.h"

namespace terrier::binder {

BindNodeVisitor::BindNodeVisitor(catalog::CatalogAccessor* catalog_accessor, transaction::TransactionContext *txn, std::string default_database_name)
    : catalog_accessor_(catalog_accessor), txn_(txn), default_database_name_(default_database_name) {
  // TODO:
  //  traffic cop should generate the catalog accessor and hand to the binder
  //  Since the catalog accessor has attribute txn and db id
  //  probably we should only take in the catalog accessor as parameter only?
  context_ = nullptr;
}

void BindNodeVisitor::BindNameToNode(parser::SQLStatement *tree) {
  tree->Accept(this);
}

void BindNodeVisitor::Visit(parser::SelectStatement *node) {
  // TODO: remove make shared ... Use raw pointers, as the context is not stored in the binder
  //  Statement will not be modified
  //  Expression will be modified but only alias and tupleValueExpression
  //  We can make binder friend of abstract expression
  context_ = std::make_shared<BinderContext>(context_);

  if (node->GetSelectTable() != nullptr) {
    node->GetSelectTable()->Accept(this);
  }
  if (node->GetSelectCondition() != nullptr) {
    node->GetSelectCondition()->Accept(this);
    // Derive depth for all exprs in the where clause
    node->where_clause->DeriveDepth();
    node->where_clause->DeriveSubqueryFlag();
  }
  if (node->GetSelectOrderBy() != nullptr) {
    node->GetSelectOrderBy()->Accept(this);
  }
  if (node->GetSelectLimit() != nullptr) {
    node->GetSelectLimit()->Accept(this);
  }
  if (node->GetSelectGroupBy() != nullptr) {
    node->GetSelectGroupBy()->Accept(this);
  }

  // TODO: unique_ptr? do we keep it?
  std::vector<std::unique_ptr<parser::AbstractExpression>> new_select_list;
  for (auto &select_element : node->GetSelectColumns()) {
    if (select_element->GetExpressionType() == parser::ExpressionType::STAR) {
      context_->GenerateAllColumnExpressions(new_select_list);
      continue;
    }

    select_element->Accept(this);

    // TODO: Is concept of depth completely discarded in terrier?
    // Derive depth for all exprs in the select clause
    if (select_element->GetExpressionType() == parser::ExpressionType::STAR) {
      select_element->SetDepth(context_->GetDepth());
    } else {
      select_element->DeriveDepth();
    }
    // TODO: what about has_subquery in abstract expression
    select_element->DeriveSubqueryFlag();

    // TODO:
    //  in peloton code, deduce expression type only happens in operator expression and aggregate expression
    //  in operator expression, it returns boolean for logical operations, and highest type for arithmetic operation
    //  in aggregate expression, it returns integer for count, the underlying type for min,max,sum, and decimal for ave
    // Traverse the expression to deduce expression value type and name
    select_element->DeduceExpressionType();

    // TODO:
    //  in peloton code, deduce expression name only happens in constant value expression and tuple value expression
    //  in constant value expression, it returns the string repr for the constant if alias is not defined
    //  ( still I have no idea where the alias is modified or written )
    //  in tuple_value expression, it returns the col name if alias is not defined
    //  ** In the new column value expression (split from tuple value expression) probably should be there **
    select_element->DeduceExpressionName();

    new_select_list.push_back(std::move(select_element));
  }
  node->select_list = std::move(new_select_list);
  // Temporarily discard const to update the depth
  const_cast<parser::SelectStatement *>(node)->depth = context_->GetDepth();
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
  // TODO: Select, list, join are exclusive?
  if (node->GetSelect() != nullptr) {
    if (node->GetAlias().empty()) {
      throw BINDER_EXCEPTION("Alias not found for query derived table");
    }

    // Save the previous context
    auto pre_context = context_;
    node->GetSelect()->Accept(this);
    // Restore the previous level context
    context_ = pre_context;
    // Add the table to the current context at the end
    // TODO: there are shared_ptr in the selectStatement
    context_->AddNestedTable(node->GetAlias(), node->GetSelect()->GetSelectColumns());
  }
    // Join
  else if (node->GetJoin() != nullptr) {
    node->GetJoin()->Accept(this);
    // Multiple tables
  }

  else if (!node->GetList().empty()) {
    for (auto &table : node->GetList()) table->Accept(this);
  }
    // Single table
  else {
    context_->AddRegularTable(txn_, node, default_database_name_);
  }
}

void BindNodeVisitor::Visit(parser::GroupByDescription *node) {
  for (auto &col : node->GetColumns()) {
    col->Accept(this);
  }
  if (node->GetHaving() != nullptr) {
    node->GetHaving()->Accept(this);
  }
}
void BindNodeVisitor::Visit(parser::OrderByDescription *node) {
  for (auto &expr : node->GetOrderByExpressions()) {
    if (expr != nullptr) {
      expr->Accept(this);
    }
  }
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
  // node->TryBindDatabaseName(default_database_name_);
  context_->AddRegularTable(txn_, node->GetDeletionTable()->GetDatabaseName(), node->GetDeletionTable()->GetSchemaName(),
                            node->GetDeletionTable()->GetTableName(), node->GetDeletionTable()->GetTableName());

  if (node->GetDeleteCondition() != nullptr) {
    node->GetDeleteCondition()->Accept(this);
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::LimitDescription *) {}

void BindNodeVisitor::Visit(parser::CopyStatement *node) {
  context_ = std::make_shared<BinderContext>(nullptr);
  if (node->GetCopyTable() != nullptr) {
    node->GetCopyTable()->Accept(this);

    // If the table is given, we're either writing or reading all columns
    context_->GenerateAllColumnExpressions(node->GetSelectStatement()->GetSelectColumns());
  } else {
    node->GetSelectStatement()->Accept(this);
  }
}

void BindNodeVisitor::Visit(parser::CreateFunctionStatement *) {}
void BindNodeVisitor::Visit(parser::CreateStatement *node) {
  // TODO: try nothing.
  //  Try  bind database name will set, for the table_ref of the node,
  //  the db_name and schema_name to default if they are not there
  node->TryBindDatabaseName(default_database_name_);
}
void BindNodeVisitor::Visit(parser::InsertStatement *node) {
  node->TryBindDatabaseName(default_database_name_);
  context_ = std::make_shared<BinderContext>(nullptr);
  context_->AddRegularTable(node->GetDatabaseName(), node->GetSchemaName(),
                            node->GetTableName(), node->GetTableName(), txn_);
  if (node->select != nullptr) {
    node->select->Accept(this);
  }
  context_ = nullptr;
}
void BindNodeVisitor::Visit(parser::DropStatement *node) {
  node->TryBindDatabaseName(default_database_name_);
}
void BindNodeVisitor::Visit(parser::PrepareStatement *) {}
void BindNodeVisitor::Visit(parser::ExecuteStatement *) {}
void BindNodeVisitor::Visit(parser::TransactionStatement *) {}
void BindNodeVisitor::Visit(parser::AnalyzeStatement *node) {
  node->TryBindDatabaseName(default_database_name_);
}

// void BindNodeVisitor::Visit(const parser::ConstantValueExpression *) {}

void BindNodeVisitor::Visit(parser::TupleValueExpression *expr) {
  // TODO: create/copy the tupleValueExpression
  if (!expr->GetIsBound()) {
    std::tuple<oid_t, oid_t, oid_t> col_pos_tuple;
    std::shared_ptr<catalog::Schema> schema = nullptr;
    type::TypeId value_type;
    int depth = -1;

    std::string table_name = expr->GetTableName();
    std::string col_name = expr->GetColumnName();

    // Convert all the names to lower cases
    std::transform(table_name.begin(), table_name.end(), table_name.begin(), ::tolower);
    std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::tolower);

    // Table name not specified in the expression. Loop through all the table in the binder context.
    if (table_name.empty()) {
      if (!BinderContext::GetColumnPosTuple(context_, col_name, col_pos_tuple, table_name, value_type, depth)) {
        throw BINDER_EXCEPTION(("Cannot find column " + col_name).c_str());
      }
      expr->SetTableName(table_name);
    }
      // Table name is present
    else {
      // Regular table
      // TODO: we have changed table_obj to schema
      //  in this function, table name and schema are filled in
      if (BinderContext::GetRegularTableObj(context_, table_name, schema, depth)) {
        if (!BinderContext::GetColumnPosTuple(col_name, schema, col_pos_tuple, value_type)) {
          throw BINDER_EXCEPTION(("Cannot find column " + col_name).c_str());
        }
      }
        // Nested table
      else if (!BinderContext::CheckNestedTableColumn(context_, table_name, col_name, value_type, depth))
        throw BINDER_EXCEPTION(("Invalid table reference " + expr->GetTableName()).c_str());
    }
    PELOTON_ASSERT(!expr->GetIsBound());
    expr->SetDepth(depth);
    expr->SetColName(col_name);
    expr->SetValueType(value_type);
    expr->SetBoundOid(col_pos_tuple);
  }
}

void BindNodeVisitor::Visit(parser::CaseExpression *expr) {
  for (size_t i = 0; i < expr->GetWhenClauseSize(); ++i) {
    expr->GetWhenClauseCond(i)->Accept(this);
  }
}

void BindNodeVisitor::Visit(parser::SubqueryExpression *expr) {
  expr->GetSubSelect()->Accept(this);
}

void BindNodeVisitor::Visit(parser::StarExpression *expr) {
  if (!BinderContext::HasTables(context_)) {
    throw BinderException("Invalid expression" + expr->GetInfo());
  }
}

// Deduce value type for these expressions
void BindNodeVisitor::Visit(parser::OperatorExpression *expr) {
  SqlNodeVisitor::Visit(expr);
  expr->DeduceExpressionType();
}
void BindNodeVisitor::Visit(parser::AggregateExpression *expr) {
  SqlNodeVisitor::Visit(expr);
  expr->DeduceExpressionType();
}

void BindNodeVisitor::Visit(parser::FunctionExpression *expr) {
  // Visit the subtree first
  SqlNodeVisitor::Visit(expr);

  // Check catalog and bind function
  std::vector<type::TypeId> argtypes;
  for (size_t i = 0; i < expr->GetChildrenSize(); i++)
    argtypes.push_back(expr->GetChild(i)->GetValueType());
  // Check and set the function ptr
  // TODO(boweic): Visit the catalog using the interface that is protected by
  // transaction
  const catalog::FunctionData &func_data =
      catalog_accessor_->GetFunction(expr->GetFuncName(), argtypes);
  LOG_DEBUG("Function %s found in the catalog", func_data.func_name_.c_str());
  LOG_DEBUG("Argument num: %ld", func_data.argument_types_.size());
  LOG_DEBUG("Is UDF %d", func_data.is_udf_);

  if (!func_data.is_udf_) {
    expr->SetBuiltinFunctionExpressionParameters(
        func_data.func_, func_data.return_type_, func_data.argument_types_);

    // Look into the OperatorId for built-in functions to check the first
    // argument for timestamp functions.
    // TODO(LM): The OperatorId might need to be changed to global ID after we
    // rewrite the function identification logic.
    auto func_operator_id = func_data.func_.op_id;
    if (func_operator_id == OperatorId::DateTrunc ||
        func_operator_id == OperatorId::DatePart) {
      auto date_part = expr->GetChild(0);

      // Test whether the first argument is a correct DatePartType
      StringToDatePartType(
          date_part->Evaluate(nullptr, nullptr, nullptr).ToString());
    }
  } else {
    expr->SetUDFFunctionExpressionParameters(func_data.func_context_,
                                             func_data.return_type_,
                                             func_data.argument_types_);
  }
}

}  // namespace binder
}  // namespace terrier
