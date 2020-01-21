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
#include "loggers/binder_logger.h"
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

BindNodeVisitor::BindNodeVisitor(common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor,
                                 std::string default_database_name)
    : catalog_accessor_(catalog_accessor), default_database_name_(std::move(default_database_name)) {}

void BindNodeVisitor::BindNameToNode(common::ManagedPointer<parser::SQLStatement> tree,
                                     parser::ParseResult *parse_result) {
  tree->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(parser::SelectStatement *node, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting SelectStatement ...");
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
  BINDER_LOG_DEBUG("Gathering select columns...");
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
  BINDER_LOG_DEBUG("Visiting JoinDefinition ...");
  // The columns in join condition can only bind to the join tables
  node->GetLeftTable()->Accept(this, parse_result);
  node->GetRightTable()->Accept(this, parse_result);
  node->GetJoinCondition()->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(parser::TableRef *node, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting TableRef ...");
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
    if (catalog_accessor_->GetTableOid(node->GetTableName()) == catalog::INVALID_TABLE_OID) {
      throw BINDER_EXCEPTION("Accessing non-existing table.");
    }
    context_->AddRegularTable(catalog_accessor_, node);
  }
}

void BindNodeVisitor::Visit(parser::GroupByDescription *node, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting GroupByDescription ...");
  for (auto &col : node->GetColumns()) col->Accept(this, parse_result);
  if (node->GetHaving() != nullptr) node->GetHaving()->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(parser::OrderByDescription *node, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting OrderByDescription ...");
  for (auto &expr : node->GetOrderByExpressions())
    if (expr != nullptr) expr->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(parser::UpdateStatement *node, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting UpdateStatement ...");
  context_ = new BinderContext(nullptr);

  node->GetUpdateTable()->Accept(this, parse_result);
  if (node->GetUpdateCondition() != nullptr) node->GetUpdateCondition()->Accept(this, parse_result);
  for (auto &update : node->GetUpdateClauses()) {
    update->GetUpdateValue()->Accept(this, parse_result);
  }

  delete context_;
  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::DeleteStatement *node, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting DeleteStatement ...");
  context_ = new BinderContext(nullptr);
  node->GetDeletionTable()->TryBindDatabaseName(default_database_name_);
  auto table = node->GetDeletionTable();
  context_->AddRegularTable(catalog_accessor_, table->GetDatabaseName(), table->GetNamespaceName(),
                            table->GetTableName(), table->GetTableName());

  if (node->GetDeleteCondition() != nullptr) {
    node->GetDeleteCondition()->Accept(this, parse_result);
  }

  delete context_;
  context_ = nullptr;
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::LimitDescription *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting LimitDescription ...");
}

void BindNodeVisitor::Visit(parser::CopyStatement *node, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting CopyStatement ...");
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

  delete context_;
  context_ = nullptr;
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::CreateFunctionStatement *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting CreateFunctionStatement ...");
}

void BindNodeVisitor::Visit(parser::CreateStatement *node, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting CreateStatement ...");
  context_ = new BinderContext(context_);

  auto create_type = node->GetCreateType();
  switch (create_type) {
    case parser::CreateStatement::CreateType::kDatabase:
      if (catalog_accessor_->GetDatabaseOid(node->GetDatabaseName()) != catalog::INVALID_DATABASE_OID) {
        throw BINDER_EXCEPTION("Database name already exists");
      }
      break;
    case parser::CreateStatement::CreateType::kTable:
      node->TryBindDatabaseName(default_database_name_);
      if (catalog_accessor_->GetTableOid(node->GetTableName()) != catalog::INVALID_TABLE_OID) {
        throw BINDER_EXCEPTION("Table name already exists");
      }
      context_->AddNewTable(node->GetTableName(), node->GetColumns());
      for (const auto &col : node->GetColumns()) {
        if (col->GetDefaultExpression() != nullptr) col->GetDefaultExpression()->Accept(this, parse_result);
        if (col->GetCheckExpression() != nullptr) col->GetCheckExpression()->Accept(this, parse_result);
      }
      for (const auto &fk : node->GetForeignKeys()) {
        // foreign key does not have check exprssion nor default expression
        auto table_oid = catalog_accessor_->GetTableOid(fk->GetForeignKeySinkTableName());
        if (table_oid == catalog::INVALID_TABLE_OID) {
          throw BINDER_EXCEPTION("Foreign key referencing non-existing table");
        }

        auto src = fk->GetForeignKeySources();
        auto ref = fk->GetForeignKeySinks();

        // TODO(Ling): assuming no composite key? Do we support create type?
        //  Where should we check uniqueness constraint
        if (src.size() != ref.size())
          throw BINDER_EXCEPTION("Number of columns in foreign key does not match number of reference columns");

        for (size_t i = 0; i < src.size(); i++) {
          auto ref_col = catalog_accessor_->GetSchema(table_oid).GetColumn(ref[i]);
          if (ref_col.Oid() == catalog::INVALID_COLUMN_OID) {
            throw BINDER_EXCEPTION("Foreign key referencing non-existing column");
          }

          bool find = false;
          for (const auto &col : node->GetColumns()) {
            if (col->GetColumnName() == src[i]) {
              find = true;

              // check if their type matches
              if (ref_col.Type() != col->GetValueType())
                throw BINDER_EXCEPTION(
                    ("Foreign key source column " + src[i] + "type does not match reference column type").c_str());

              break;
            }
          }
          if (!find) throw BINDER_EXCEPTION(("Cannot find column " + src[i] + " in foreign key source").c_str());
        }
      }
      break;
    case parser::CreateStatement::CreateType::kIndex:
      if (catalog_accessor_->GetTableOid(node->GetTableName()) == catalog::INVALID_TABLE_OID) {
        throw BINDER_EXCEPTION("Build index on non-existing table.");
      }
      if (catalog_accessor_->GetIndexOid(node->GetIndexName()) != catalog::INVALID_INDEX_OID) {
        throw BINDER_EXCEPTION("This index already exists.");
      }
      node->TryBindDatabaseName(default_database_name_);
      context_->AddRegularTable(catalog_accessor_, node->GetDatabaseName(), node->GetNamespaceName(),
                                node->GetTableName(), node->GetTableName());

      for (auto &attr : node->GetIndexAttributes()) {
        if (attr.HasExpr()) {
          attr.GetExpression()->Accept(this, parse_result);
        } else {
          // TODO(Matt): can an index attribute definition ever reference multiple tables? I don't think so. We should
          // probably move this out of the loop.
          auto tb_oid = catalog_accessor_->GetTableOid(node->GetTableName());
          if (!BinderContext::ColumnInSchema(catalog_accessor_->GetSchema(tb_oid), attr.GetName()))
            throw BINDER_EXCEPTION(("No such column specified by the index attribute " + attr.GetName()).c_str());
        }
      }
      break;
    case parser::CreateStatement::CreateType::kTrigger:
      node->TryBindDatabaseName(default_database_name_);
      context_->AddRegularTable(catalog_accessor_, node->GetDatabaseName(), node->GetNamespaceName(),
                                node->GetTableName(), node->GetTableName());
      // TODO(Ling): I think there are rules on when the trigger can have OLD reference
      //  and when it can have NEW reference, but I'm not sure how it actually works... need to add those check later
      context_->AddRegularTable(catalog_accessor_, node->GetDatabaseName(), node->GetNamespaceName(),
                                node->GetTableName(), "old");
      context_->AddRegularTable(catalog_accessor_, node->GetDatabaseName(), node->GetNamespaceName(),
                                node->GetTableName(), "new");
      if (node->GetTriggerWhen() != nullptr) node->GetTriggerWhen()->Accept(this, parse_result);
      break;
    case parser::CreateStatement::CreateType::kSchema:
      // nothing for binder to handler
      break;
    case parser::CreateStatement::CreateType::kView:
      node->TryBindDatabaseName(default_database_name_);
      TERRIER_ASSERT(node->GetViewQuery() != nullptr, "View requires a query");
      node->GetViewQuery()->Accept(this, parse_result);
      break;
  }
  auto curr_context = context_;
  context_ = context_->GetUpperContext();
  delete curr_context;
}

void BindNodeVisitor::Visit(parser::InsertStatement *node, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting InsertStatement ...");
  context_ = new BinderContext(nullptr);
  node->GetInsertionTable()->TryBindDatabaseName(default_database_name_);

  auto table = node->GetInsertionTable();
  context_->AddRegularTable(catalog_accessor_, table->GetDatabaseName(), table->GetNamespaceName(),
                            table->GetTableName(), table->GetTableName());
  if (node->GetSelect() != nullptr) node->GetSelect()->Accept(this, parse_result);

  delete context_;
  context_ = nullptr;
}

void BindNodeVisitor::Visit(parser::DropStatement *node, UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting DropStatement ...");
  context_ = new BinderContext(context_);

  auto drop_type = node->GetDropType();
  switch (drop_type) {
    case parser::DropStatement::DropType::kDatabase:
      if (catalog_accessor_->GetDatabaseOid(node->GetDatabaseName()) == catalog::INVALID_DATABASE_OID) {
        throw BINDER_EXCEPTION("Database does not exist");
      }
      break;
    case parser::DropStatement::DropType::kTable:
      node->TryBindDatabaseName(default_database_name_);
      if (catalog_accessor_->GetTableOid(node->GetTableName()) == catalog::INVALID_TABLE_OID) {
        throw BINDER_EXCEPTION("Table does not exist");
      }
      break;
    case parser::DropStatement::DropType::kIndex:
      node->TryBindDatabaseName(default_database_name_);
      if (catalog_accessor_->GetIndexOid(node->GetIndexName()) == catalog::INVALID_INDEX_OID) {
        throw BINDER_EXCEPTION("Index does not exist");
      }
      break;
    case parser::DropStatement::DropType::kTrigger:
      // TODO(Ling): Get Trigger OID in catalog?
    case parser::DropStatement::DropType::kSchema:
    case parser::DropStatement::DropType::kView:
      // TODO(Ling): Get View OID in catalog?
    case parser::DropStatement::DropType::kPreparedStatement:
      break;
  }
  auto curr_context = context_;
  context_ = context_->GetUpperContext();
  delete curr_context;
}
void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::PrepareStatement *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting PrepareStatement ...");
}
void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::ExecuteStatement *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting ExecuteStatement ...");
}
void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::TransactionStatement *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting TransactionStatement ...");
}
void BindNodeVisitor::Visit(parser::AnalyzeStatement *node, UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting AnalyzeStatement ...");
  node->GetAnalyzeTable()->TryBindDatabaseName(default_database_name_);
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::ConstantValueExpression *expr,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting ConstantValueExpression ...");
}

void BindNodeVisitor::Visit(parser::ColumnValueExpression *expr, UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting ColumnValueExpression ...");
  // TODO(Ling): consider remove precondition check if the *_oid_ will never be initialized till binder
  //  That is, the object would not be initialized using ColumnValueExpression(database_oid, table_oid, column_oid)
  //  at this point
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
  BINDER_LOG_DEBUG("Visiting CaseExpression ...");
  for (size_t i = 0; i < expr->GetWhenClauseSize(); ++i) {
    expr->GetWhenClauseCondition(i)->Accept(this, parse_result);
  }
}

void BindNodeVisitor::Visit(parser::SubqueryExpression *expr, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting SubqueryExpression ...");
  expr->GetSubselect()->Accept(this, parse_result);
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::StarExpression *expr,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting StarExpression ...");
  if (context_ == nullptr || !context_->HasTables()) {
    throw BINDER_EXCEPTION("Invalid [Expression :: STAR].");
  }
}

// Derive value type for these expressions
void BindNodeVisitor::Visit(parser::OperatorExpression *expr, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting OperatorExpression ...");
  SqlNodeVisitor::Visit(expr, parse_result);
  expr->DeriveReturnValueType();
}
void BindNodeVisitor::Visit(parser::AggregateExpression *expr, parser::ParseResult *parse_result) {
  BINDER_LOG_DEBUG("Visiting AggregateExpression ...");
  SqlNodeVisitor::Visit(expr, parse_result);
  expr->DeriveReturnValueType();
}
}  // namespace terrier::binder
