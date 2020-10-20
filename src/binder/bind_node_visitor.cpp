#include "binder/bind_node_visitor.h"

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "binder/binder_context.h"
#include "binder/binder_sherpa.h"
#include "binder/binder_util.h"
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "common/error/exception.h"
#include "common/managed_pointer.h"
#include "execution/functions/function_context.h"
#include "loggers/binder_logger.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/table_star_expression.h"
#include "parser/expression/type_cast_expression.h"
#include "parser/statements.h"

namespace terrier::binder {

/*
 * TODO(WAN): Note that some functions invoke SqlNodeVisitor::Visit() twice.
 * This is necessary and is IMO an argument for the binder refactor that we want to do.
 *
 * The overall structure of the sherpa-augmented visited pattern is:
 * SqlNodeVisitor::Visit #1: each node resolves its own type, e.g. ColumnValue comes in as INVALID, gets schema type.
 * BinderSherpa now uses the fully resolved nodes to set desired types, e.g. casting string to timestamp for ==.
 * SqlNodeVisitor::Visit #2: each node uses the sherpa's information to convert itself accordingly.
 *
 * Practically, this means that any time you use a sherpa_->SetDesiredType(), you must invoke Visit again.
 */

BindNodeVisitor::BindNodeVisitor(const common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor,
                                 const catalog::db_oid_t db_oid)
    : catalog_accessor_(catalog_accessor), db_oid_(db_oid) {}

void BindNodeVisitor::BindNameToNode(
    common::ManagedPointer<parser::ParseResult> parse_result,
    const common::ManagedPointer<std::vector<parser::ConstantValueExpression>> parameters,
    const common::ManagedPointer<std::vector<type::TypeId>> desired_parameter_types) {
  TERRIER_ASSERT(parse_result != nullptr, "We shouldn't be tring to bind something without a ParseResult.");
  sherpa_ = std::make_unique<BinderSherpa>(parse_result, parameters, desired_parameter_types);
  TERRIER_ASSERT(sherpa_->GetParseResult()->GetStatements().size() == 1, "Binder can only bind one at a time.");
  sherpa_->GetParseResult()->GetStatement(0)->Accept(
      common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::AnalyzeStatement> node) {
  BINDER_LOG_TRACE("Visiting AnalyzeStatement ...");
  SqlNodeVisitor::Visit(node);

  InitTableRef(node->GetAnalyzeTable());
  ValidateDatabaseName(node->GetAnalyzeTable()->GetDatabaseName());
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::CopyStatement> node) {
  BINDER_LOG_TRACE("Visiting CopyStatement ...");
  SqlNodeVisitor::Visit(node);

  TERRIER_ASSERT(context_ == nullptr, "COPY should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  if (node->GetCopyTable() != nullptr) {
    node->GetCopyTable()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());

    // If the table is given, we're either writing or reading all columns
    parser::TableStarExpression table_star = parser::TableStarExpression();
    std::vector<common::ManagedPointer<parser::AbstractExpression>> new_select_list;
    context_->GenerateAllColumnExpressions(common::ManagedPointer<parser::TableStarExpression>(&table_star),
                                           sherpa_->GetParseResult(), common::ManagedPointer(&new_select_list));
    auto col = node->GetSelectStatement()->GetSelectColumns();
    col.insert(std::end(col), std::begin(new_select_list), std::end(new_select_list));
  } else {
    node->GetSelectStatement()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::CreateFunctionStatement> node) {
  BINDER_LOG_TRACE("Visiting CreateFunctionStatement ...");
  SqlNodeVisitor::Visit(node);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::CreateStatement> node) {
  BINDER_LOG_TRACE("Visiting CreateStatement ...");
  SqlNodeVisitor::Visit(node);

  TERRIER_ASSERT(context_ == nullptr, "CREATE should be a root (INSERT into CREATE?).");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  auto create_type = node->GetCreateType();
  switch (create_type) {
    case parser::CreateStatement::CreateType::kDatabase:
      if (catalog_accessor_->GetDatabaseOid(node->GetDatabaseName()) != catalog::INVALID_DATABASE_OID) {
        throw BINDER_EXCEPTION(fmt::format("database \"{}\" already exists", node->GetDatabaseName()),
                               common::ErrorCode::ERRCODE_DUPLICATE_DATABASE);
      }
      break;
    case parser::CreateStatement::CreateType::kTable:
      ValidateDatabaseName(node->GetDatabaseName());

      if (catalog_accessor_->GetTableOid(node->GetTableName()) != catalog::INVALID_TABLE_OID) {
        throw BINDER_EXCEPTION(fmt::format("relation \"{}\" already exists", node->GetTableName()),
                               common::ErrorCode::ERRCODE_DUPLICATE_TABLE);
      }
      context_->AddNewTable(node->GetTableName(), node->GetColumns());
      for (const auto &col : node->GetColumns()) {
        if (col->GetDefaultExpression() != nullptr)
          col->GetDefaultExpression()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
        if (col->GetCheckExpression() != nullptr)
          col->GetCheckExpression()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
      }
      for (const auto &fk : node->GetForeignKeys()) {
        // foreign key does not have check exprssion nor default expression
        auto table_oid = catalog_accessor_->GetTableOid(fk->GetForeignKeySinkTableName());
        if (table_oid == catalog::INVALID_TABLE_OID) {
          throw BINDER_EXCEPTION("Foreign key referencing non-existing table",
                                 common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
        }

        auto src = fk->GetForeignKeySources();
        auto ref = fk->GetForeignKeySinks();

        // TODO(Ling): assuming no composite key? Do we support create type?
        //  Where should we check uniqueness constraint
        if (src.size() != ref.size())
          throw BINDER_EXCEPTION("Number of columns in foreign key does not match number of reference columns",
                                 common::ErrorCode::ERRCODE_INVALID_FOREIGN_KEY);

        for (size_t i = 0; i < src.size(); i++) {
          auto ref_col = catalog_accessor_->GetSchema(table_oid).GetColumn(ref[i]);
          if (ref_col.Oid() == catalog::INVALID_COLUMN_OID) {
            throw BINDER_EXCEPTION("Foreign key referencing non-existing column",
                                   common::ErrorCode::ERRCODE_INVALID_FOREIGN_KEY);
          }

          bool find = false;
          for (const auto &col : node->GetColumns()) {
            if (col->GetColumnName() == src[i]) {
              find = true;

              // check if their type matches
              if (ref_col.Type() != col->GetValueType())
                throw BINDER_EXCEPTION(
                    fmt::format("Foreign key source column {} type does not match reference column type", src[i]),
                    common::ErrorCode::ERRCODE_INVALID_FOREIGN_KEY);

              break;
            }
          }
          if (!find)
            throw BINDER_EXCEPTION(fmt::format("Cannot find column {} in foreign key source", src[i]),
                                   common::ErrorCode::ERRCODE_INVALID_FOREIGN_KEY);
        }
      }
      break;
    case parser::CreateStatement::CreateType::kIndex:
      ValidateDatabaseName(node->GetDatabaseName());
      if (catalog_accessor_->GetTableOid(node->GetTableName()) == catalog::INVALID_TABLE_OID) {
        throw BINDER_EXCEPTION("Build index on non-existing table.", common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
      }
      if (catalog_accessor_->GetIndexOid(node->GetIndexName()) != catalog::INVALID_INDEX_OID) {
        throw BINDER_EXCEPTION("This index already exists.", common::ErrorCode::ERRCODE_DUPLICATE_OBJECT);
      }
      context_->AddRegularTable(catalog_accessor_, db_oid_, node->GetNamespaceName(), node->GetTableName(),
                                node->GetTableName());

      for (auto &attr : node->GetIndexAttributes()) {
        if (attr.HasExpr()) {
          attr.GetExpression()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
        } else {
          // TODO(Matt): can an index attribute definition ever reference multiple tables? I don't think so. We should
          // probably move this out of the loop.
          auto tb_oid = catalog_accessor_->GetTableOid(node->GetTableName());
          if (!BinderContext::ColumnInSchema(catalog_accessor_->GetSchema(tb_oid), attr.GetName()))
            throw BINDER_EXCEPTION(fmt::format("No such column specified by the index attribute {}", attr.GetName()),
                                   common::ErrorCode::ERRCODE_INVALID_OBJECT_DEFINITION);
        }
      }
      break;
    case parser::CreateStatement::CreateType::kTrigger:
      ValidateDatabaseName(node->GetDatabaseName());
      context_->AddRegularTable(catalog_accessor_, db_oid_, node->GetNamespaceName(), node->GetTableName(),
                                node->GetTableName());
      // TODO(Ling): I think there are rules on when the trigger can have OLD reference
      //  and when it can have NEW reference, but I'm not sure how it actually works... need to add those check later
      context_->AddRegularTable(catalog_accessor_, db_oid_, node->GetNamespaceName(), node->GetTableName(), "old");
      context_->AddRegularTable(catalog_accessor_, db_oid_, node->GetNamespaceName(), node->GetTableName(), "new");
      if (node->GetTriggerWhen() != nullptr)
        node->GetTriggerWhen()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
      break;
    case parser::CreateStatement::CreateType::kSchema:
      // nothing for binder to handler
      break;
    case parser::CreateStatement::CreateType::kView:
      ValidateDatabaseName(node->GetDatabaseName());
      TERRIER_ASSERT(node->GetViewQuery() != nullptr, "View requires a query");
      node->GetViewQuery()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
      break;
  }

  context_ = context_->GetUpperContext();
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::DeleteStatement> node) {
  BINDER_LOG_TRACE("Visiting DeleteStatement ...");
  SqlNodeVisitor::Visit(node);

  TERRIER_ASSERT(context_ == nullptr, "DELETE should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  InitTableRef(node->GetDeletionTable());
  ValidateDatabaseName(node->GetDeletionTable()->GetDatabaseName());

  auto table = node->GetDeletionTable();
  context_->AddRegularTable(catalog_accessor_, db_oid_, table->GetNamespaceName(), table->GetTableName(),
                            table->GetTableName());

  if (node->GetDeleteCondition() != nullptr) {
    node->GetDeleteCondition()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::DropStatement> node) {
  BINDER_LOG_TRACE("Visiting DropStatement ...");
  SqlNodeVisitor::Visit(node);

  TERRIER_ASSERT(context_ == nullptr, "DROP should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  auto drop_type = node->GetDropType();
  switch (drop_type) {
    case parser::DropStatement::DropType::kDatabase:
      ValidateDatabaseName(node->GetDatabaseName());
      break;
    case parser::DropStatement::DropType::kTable:
      ValidateDatabaseName(node->GetDatabaseName());
      if (catalog_accessor_->GetTableOid(node->GetTableName()) == catalog::INVALID_TABLE_OID) {
        throw BINDER_EXCEPTION(fmt::format("relation \"{}\" does not exist", node->GetTableName()),
                               common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
      }
      break;
    case parser::DropStatement::DropType::kIndex:
      ValidateDatabaseName(node->GetDatabaseName());
      if (catalog_accessor_->GetIndexOid(node->GetIndexName()) == catalog::INVALID_INDEX_OID) {
        throw BINDER_EXCEPTION(fmt::format("index \"{}\" does not exist", node->GetTableName()),
                               common::ErrorCode::ERRCODE_UNDEFINED_OBJECT);
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

  context_ = nullptr;
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::ExecuteStatement> node) {
  BINDER_LOG_TRACE("Visiting ExecuteStatement ...");
  SqlNodeVisitor::Visit(node);
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::ExplainStatement> node) {
  BINDER_LOG_TRACE("Visiting ExplainStatement ...");
  SqlNodeVisitor::Visit(node);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::InsertStatement> node) {
  BINDER_LOG_TRACE("Visiting InsertStatement ...");
  SqlNodeVisitor::Visit(node);

  TERRIER_ASSERT(context_ == nullptr, "INSERT should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  InitTableRef(node->GetInsertionTable());
  ValidateDatabaseName(node->GetInsertionTable()->GetDatabaseName());

  auto table = node->GetInsertionTable();
  context_->AddRegularTable(catalog_accessor_, db_oid_, table->GetNamespaceName(), table->GetTableName(),
                            table->GetTableName());

  if (node->GetSelect() != nullptr) {  // INSERT FROM SELECT
    node->GetSelect()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  } else {  // RAW INSERT
    // Perform input validation and parsing of strings into dates.
    auto binder_table_data = context_->GetTableMapping(table->GetTableName());
    const auto &table_schema = std::get<2>(*binder_table_data);

    auto insert_columns = node->GetInsertColumns();
    // Validate input columns.
    {
      // Test that all the insert columns exist.
      for (const auto &col : *insert_columns) {
        if (!BinderContext::ColumnInSchema(table_schema, col)) {
          throw BINDER_EXCEPTION("Insert column does not exist", common::ErrorCode::ERRCODE_UNDEFINED_COLUMN);
        }
      }
    }

    auto num_schema_columns = table_schema.GetColumns().size();
    auto num_insert_columns = insert_columns->size();  // If unspecified by query, insert_columns is length 0.
    auto insert_values = node->GetValues();
    // Validate input values.
    {
      for (auto &values : *insert_values) {
        // Value is a row (tuple) to insert.
        size_t num_values = values.size();
        // Test that they have the same number of columns.
        {
          bool is_insert_cols_specified = num_insert_columns != 0;
          bool insert_cols_ok = is_insert_cols_specified && num_values == num_insert_columns;
          bool insert_schema_ok = !is_insert_cols_specified && num_values == num_schema_columns;
          if (!(insert_cols_ok || insert_schema_ok)) {
            throw BINDER_EXCEPTION("Mismatch in number of insert columns and number of insert values.",
                                   common::ErrorCode::ERRCODE_SYNTAX_ERROR);
          }
        }

        std::vector<std::pair<catalog::Schema::Column, common::ManagedPointer<parser::AbstractExpression>>> cols;

        if (num_insert_columns == 0) {
          // If the number of insert columns is zero, it is assumed that the tuple values are already schema ordered.
          for (size_t i = 0; i < num_values; i++) {
            auto pair = std::make_pair(table_schema.GetColumns()[i], values[i]);
            cols.emplace_back(pair);
          }
        } else {
          // Otherwise, some insert columns were specified. Potentially not all and potentially out of order.
          for (auto &schema_col : table_schema.GetColumns()) {
            auto it = std::find(insert_columns->begin(), insert_columns->end(), schema_col.Name());
            // Find the index of the current schema column.
            if (it != insert_columns->end()) {
              // TODO(harsh): This might need refactoring if it becomes a performance bottleneck.
              auto index = std::distance(insert_columns->begin(), it);
              auto pair = std::make_pair(schema_col, values[index]);
              cols.emplace_back(pair);
            } else {
              // Make a null value of the right type that we can either compare with the stored expression or insert.
              auto null_ex =
                  std::make_unique<parser::ConstantValueExpression>(schema_col.Type(), execution::sql::Val(true));

              // TODO(WAN): We thought that you might be able to collapse these two cases into one, since currently
              // the catalog column's stored expression is always a NULL of the right type if not otherwise specified.
              // However, this seems to make assumptions about the current implementation in plan_generator and also
              // we want to throw an error if it is a non-NULLable column. We can leave it as it is right now.

              // If the current schema column's index was not found, that means it was not specified by the user.
              if (*schema_col.StoredExpression() != *null_ex) {
                // First, check if there is a default value for that column.
                std::unique_ptr<parser::AbstractExpression> cur_value = schema_col.StoredExpression()->Copy();
                auto pair = std::make_pair(schema_col, common::ManagedPointer(cur_value));
                cols.emplace_back(pair);
                sherpa_->GetParseResult()->AddExpression(std::move(cur_value));
              } else if (schema_col.Nullable()) {
                // If there is no default value, check if the column is NULLable, meaning we can insert a NULL.
                auto null_ex_mp = common::ManagedPointer(null_ex).CastManagedPointerTo<parser::AbstractExpression>();
                auto pair = std::make_pair(schema_col, null_ex_mp);
                cols.emplace_back(pair);
                // Note that in this case, we must move null_ex as we have taken a managed pointer to it.
                sherpa_->GetParseResult()->AddExpression(std::move(null_ex));
              } else {
                // If none of the above cases could provide a value to be inserted, then we fail.
                throw BINDER_EXCEPTION("Column not present, does not have a default and is non-nullable.",
                                       common::ErrorCode::ERRCODE_SYNTAX_ERROR);
              }
            }
          }

          // We overwrite the original insert columns and values with the schema-ordered versions generated above.
          insert_columns->clear();
          values.clear();
          for (auto &pair : cols) {
            insert_columns->emplace_back(pair.first.Name());
            values.emplace_back(pair.second);
          }
        }

        // TODO(WAN): with the sherpa, probably some of the above code can be cleaned up.

        // Perform input type transformation validation on the schema-ordered values.
        for (size_t i = 0; i < cols.size(); i++) {
          auto ins_col = cols[i].first;
          auto ins_val = cols[i].second;

          auto ret_type = ins_val->GetReturnValueType();
          auto expected_ret_type = ins_col.Type();

          // Set the desired type to be whatever the schema says the type should be.
          sherpa_->SetDesiredType(ins_val, expected_ret_type);

          auto is_default_expression = ins_val->GetExpressionType() == parser::ExpressionType::VALUE_DEFAULT;
          if (is_default_expression) {
            auto stored_expr = ins_col.StoredExpression()->Copy();
            ins_val = common::ManagedPointer(stored_expr);
            sherpa_->GetParseResult()->AddExpression(std::move(stored_expr));
          }

          auto is_cast_expression = ins_val->GetExpressionType() == parser::ExpressionType::OPERATOR_CAST;
          if (is_cast_expression) {
            if (ret_type != expected_ret_type) {
              throw BINDER_EXCEPTION("BindNodeVisitor tried to cast, but cast result type does not match the schema.",
                                     common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            }
            auto child = ins_val->GetChild(0)->Copy();
            ins_val = common::ManagedPointer(child);
            // The child should have the expected return type from the CAST parent.
            sherpa_->SetDesiredType(ins_val, expected_ret_type);
            sherpa_->GetParseResult()->AddExpression(std::move(child));
          }

          ins_val->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
          values[i] = ins_val;
        }
      }
    }
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::PrepareStatement> node) {
  BINDER_LOG_TRACE("Visiting PrepareStatement ...");
  SqlNodeVisitor::Visit(node);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::SelectStatement> node) {
  BINDER_LOG_TRACE("Visiting SelectStatement ...");
  SqlNodeVisitor::Visit(node);

  BinderContext context(context_);
  context_ = common::ManagedPointer(&context);

  if (node->GetSelectTable() != nullptr)
    node->GetSelectTable()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());

  if (node->GetSelectCondition() != nullptr) {
    node->GetSelectCondition()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
    node->GetSelectCondition()->DeriveDepth();
    node->GetSelectCondition()->DeriveSubqueryFlag();
  }

  if (node->GetSelectLimit() != nullptr)
    node->GetSelectLimit()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());

  if (node->GetSelectGroupBy() != nullptr)
    node->GetSelectGroupBy()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());

  std::vector<common::ManagedPointer<parser::AbstractExpression>> new_select_list;
  BINDER_LOG_TRACE("Gathering select columns...");
  for (auto &select_element : node->GetSelectColumns()) {
    if (select_element->GetExpressionType() == parser::ExpressionType::TABLE_STAR) {
      context_->GenerateAllColumnExpressions(select_element.CastManagedPointerTo<parser::TableStarExpression>(),
                                             sherpa_->GetParseResult(), common::ManagedPointer(&new_select_list));
      continue;
    }

    select_element->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());

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

  if (node->GetSelectOrderBy() != nullptr) {
    UnifyOrderByExpression(node->GetSelectOrderBy(), node->GetSelectColumns());
    node->GetSelectOrderBy()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  }

  context_ = context_->GetUpperContext();
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::TransactionStatement> node) {
  BINDER_LOG_TRACE("Visiting TransactionStatement ...");
  SqlNodeVisitor::Visit(node);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::UpdateStatement> node) {
  BINDER_LOG_TRACE("Visiting UpdateStatement ...");
  SqlNodeVisitor::Visit(node);

  TERRIER_ASSERT(context_ == nullptr, "UPDATE should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  auto table_ref = node->GetUpdateTable();
  table_ref->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  if (node->GetUpdateCondition() != nullptr)
    node->GetUpdateCondition()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());

  auto binder_table_data = context_->GetTableMapping(table_ref->GetTableName());
  const auto &table_schema = std::get<2>(*binder_table_data);

  for (auto &update : node->GetUpdateClauses()) {
    auto expr = update->GetUpdateValue();
    auto expected_ret_type = table_schema.GetColumn(update->GetColumnName()).Type();
    auto is_cast_expression = update->GetUpdateValue()->GetExpressionType() == parser::ExpressionType::OPERATOR_CAST;

    if (is_cast_expression) {
      auto child = expr->GetChild(0)->Copy();
      if (expr->GetReturnValueType() != expected_ret_type) {
        throw BINDER_EXCEPTION("BindNodeVisitor tried to cast, but the cast result type does not match the schema.",
                               common::ErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
      }
      sherpa_->SetDesiredType(common::ManagedPointer(child), expr->GetReturnValueType());
      update->ResetValue(common::ManagedPointer(child));
      sherpa_->GetParseResult()->AddExpression(std::move(child));
      expr = update->GetUpdateValue();
    }

    sherpa_->SetDesiredType(expr, expected_ret_type);
    expr->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  }

  SqlNodeVisitor::Visit(node);

  context_ = nullptr;
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::VariableSetStatement> node) {
  BINDER_LOG_TRACE("Visiting VariableSetStatement ...");
  SqlNodeVisitor::Visit(node);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::AggregateExpression> expr) {
  BINDER_LOG_TRACE("Visiting AggregateExpression ...");
  SqlNodeVisitor::Visit(expr);
  expr->DeriveReturnValueType();
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::CaseExpression> expr) {
  BINDER_LOG_TRACE("Visiting CaseExpression ...");
  SqlNodeVisitor::Visit(expr);
  for (size_t i = 0; i < expr->GetWhenClauseSize(); ++i) {
    expr->GetWhenClauseCondition(i)->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  }
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::ColumnValueExpression> expr) {
  BINDER_LOG_TRACE("Visiting ColumnValueExpression ...");
  SqlNodeVisitor::Visit(expr);

  sherpa_->CheckDesiredType(expr.CastManagedPointerTo<parser::AbstractExpression>());

  // TODO(Ling): consider remove precondition check if the *_oid_ will never be initialized till binder
  //  That is, the object would not be initialized using ColumnValueExpression(database_oid, table_oid, column_oid)
  //  at this point
  if (expr->GetTableOid() == catalog::INVALID_TABLE_OID) {
    std::tuple<catalog::db_oid_t, catalog::table_oid_t, catalog::Schema> tuple;
    std::string table_name = expr->GetTableName();
    std::string col_name = expr->GetColumnName();
    if (table_name.empty() && col_name.empty() && expr->GetColumnOid() != catalog::INVALID_COLUMN_OID) {
      throw BINDER_EXCEPTION(fmt::format("ORDER BY position \"{}\" is not in select list",
                                         std::to_string(expr->GetColumnOid().UnderlyingValue())),
                             common::ErrorCode::ERRCODE_UNDEFINED_COLUMN);
    }
    // Convert all the names to lower cases
    std::transform(table_name.begin(), table_name.end(), table_name.begin(), ::tolower);
    std::transform(col_name.begin(), col_name.end(), col_name.begin(), ::tolower);

    // Table name not specified in the expression. Loop through all the table in the binder context.
    if (table_name.empty()) {
      if (context_ == nullptr || !context_->SetColumnPosTuple(expr)) {
        throw BINDER_EXCEPTION(fmt::format("column \"{}\" does not exist", col_name),
                               common::ErrorCode::ERRCODE_UNDEFINED_COLUMN);
      }
    } else {
      // Table name is present
      if (context_ != nullptr && context_->GetRegularTableObj(table_name, expr, common::ManagedPointer(&tuple))) {
        if (!BinderContext::ColumnInSchema(std::get<2>(tuple), col_name)) {
          throw BINDER_EXCEPTION(fmt::format("column \"{}\" does not exist", col_name),
                                 common::ErrorCode::ERRCODE_UNDEFINED_COLUMN);
        }
        BinderContext::SetColumnPosTuple(col_name, tuple, expr);
      } else if (context_ == nullptr || !context_->CheckNestedTableColumn(table_name, col_name, expr)) {
        throw BINDER_EXCEPTION(fmt::format("Invalid table reference {}", expr->GetTableName()),
                               common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
      }
    }
  }
  // The schema is authoritative on what the type of this ColumnValueExpression should be.
  sherpa_->SetDesiredType(expr.CastManagedPointerTo<parser::AbstractExpression>(), expr->GetReturnValueType());
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::ComparisonExpression> expr) {
  BINDER_LOG_TRACE("Visiting ComparisonExpression ...");
  SqlNodeVisitor::Visit(expr);
  sherpa_->CheckDesiredType(expr.CastManagedPointerTo<parser::AbstractExpression>());
  sherpa_->SetDesiredTypePair(expr->GetChild(0), expr->GetChild(1));
  SqlNodeVisitor::Visit(expr);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::ConjunctionExpression> expr) {
  BINDER_LOG_TRACE("Visiting ConjunctionExpression ...");
  SqlNodeVisitor::Visit(expr);
  sherpa_->CheckDesiredType(expr.CastManagedPointerTo<parser::AbstractExpression>());

  for (const auto child : expr->GetChildren()) {
    sherpa_->SetDesiredType(child, type::TypeId::BOOLEAN);
  }
  SqlNodeVisitor::Visit(expr);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::ConstantValueExpression> expr) {
  BINDER_LOG_TRACE("Visiting ConstantValueExpression ...");
  SqlNodeVisitor::Visit(expr);

  const auto desired_type = sherpa_->GetDesiredType(expr.CastManagedPointerTo<parser::AbstractExpression>());
  BinderUtil::CheckAndTryPromoteType(expr, desired_type);
  expr->DeriveReturnValueType();
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::DefaultValueExpression> expr) {
  BINDER_LOG_TRACE("Visiting DefaultValueExpression ...");
  SqlNodeVisitor::Visit(expr);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::DerivedValueExpression> expr) {
  BINDER_LOG_TRACE("Visiting DerivedValueExpression ...");
  SqlNodeVisitor::Visit(expr);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::FunctionExpression> expr) {
  BINDER_LOG_TRACE("Visiting FunctionExpression ...");
  SqlNodeVisitor::Visit(expr);

  std::vector<catalog::type_oid_t> arg_types;
  auto children = expr->GetChildren();
  arg_types.reserve(children.size());
  for (const auto &child : children) {
    arg_types.push_back(catalog_accessor_->GetTypeOidFromTypeId(child->GetReturnValueType()));
  }

  auto proc_oid = catalog_accessor_->GetProcOid(expr->GetFuncName(), arg_types);
  if (proc_oid == catalog::INVALID_PROC_OID) {
    throw BINDER_EXCEPTION("Procedure not registered", common::ErrorCode::ERRCODE_UNDEFINED_FUNCTION);
  }

  auto func_context = catalog_accessor_->GetFunctionContext(proc_oid);

  expr->SetProcOid(proc_oid);
  expr->SetReturnValueType(func_context->GetFunctionReturnType());
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::OperatorExpression> expr) {
  BINDER_LOG_TRACE("Visiting OperatorExpression ...");
  SqlNodeVisitor::Visit(expr);
  expr->DeriveReturnValueType();
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::ParameterValueExpression> expr) {
  BINDER_LOG_TRACE("Visiting ParameterValueExpression ...");
  SqlNodeVisitor::Visit(expr);
  const common::ManagedPointer<parser::ConstantValueExpression> param =
      common::ManagedPointer(&((*(sherpa_->GetParameters()))[expr->GetValueIdx()]));
  const auto desired_type = sherpa_->GetDesiredType(expr.CastManagedPointerTo<parser::AbstractExpression>());

  if (desired_type != type::TypeId::INVALID) BinderUtil::CheckAndTryPromoteType(param, desired_type);

  expr->return_value_type_ = param->GetReturnValueType();
  sherpa_->SetDesiredParameterType(expr->GetValueIdx(), param->GetReturnValueType());
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::StarExpression> expr) {
  BINDER_LOG_TRACE("Visiting StarExpression ...");
  SqlNodeVisitor::Visit(expr);
  if (context_ == nullptr || !context_->HasTables()) {
    throw BINDER_EXCEPTION("Invalid [Expression :: STAR].", common::ErrorCode::ERRCODE_SYNTAX_ERROR);
  }
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::TableStarExpression> expr) {
  BINDER_LOG_TRACE("Visiting TableStarExpression ...");
  SqlNodeVisitor::Visit(expr);
  if (context_ == nullptr || !context_->HasTables()) {
    throw BINDER_EXCEPTION("Invalid [Expression :: TABLE_STAR].", common::ErrorCode::ERRCODE_SYNTAX_ERROR);
  }
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::SubqueryExpression> expr) {
  BINDER_LOG_TRACE("Visiting SubqueryExpression ...");
  SqlNodeVisitor::Visit(expr);
  expr->GetSubselect()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::TypeCastExpression> expr) {
  BINDER_LOG_TRACE("Visiting TypeCastExpression...");
  SqlNodeVisitor::Visit(expr);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::GroupByDescription> node) {
  BINDER_LOG_TRACE("Visiting GroupByDescription ...");
  SqlNodeVisitor::Visit(node);
  for (auto &col : node->GetColumns()) col->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  if (node->GetHaving() != nullptr)
    node->GetHaving()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::JoinDefinition> node) {
  BINDER_LOG_TRACE("Visiting JoinDefinition ...");
  SqlNodeVisitor::Visit(node);
  // The columns in join condition can only bind to the join tables
  node->GetLeftTable()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  node->GetRightTable()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  node->GetJoinCondition()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::LimitDescription> node) {
  BINDER_LOG_TRACE("Visiting LimitDescription ...");
  SqlNodeVisitor::Visit(node);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::OrderByDescription> node) {
  BINDER_LOG_TRACE("Visiting OrderByDescription ...");
  SqlNodeVisitor::Visit(node);
  for (auto &expr : node->GetOrderByExpressions())
    if (expr != nullptr) expr->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::TableRef> node) {
  BINDER_LOG_TRACE("Visiting TableRef ...");
  SqlNodeVisitor::Visit(node);
  InitTableRef(node);
  ValidateDatabaseName(node->GetDatabaseName());

  if (node->GetSelect() != nullptr) {
    if (node->GetAlias().empty())
      throw BINDER_EXCEPTION("Alias not found for query derived table", common::ErrorCode::ERRCODE_UNDEFINED_TABLE);

    // Save the previous context
    auto pre_context = context_;
    node->GetSelect()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
    // TODO(WAN): who exactly should save and restore contexts? Restore the previous level context
    context_ = pre_context;
    // Add the table to the current context at the end
    context_->AddNestedTable(node->GetAlias(), node->GetSelect()->GetSelectColumns());
  } else if (node->GetJoin() != nullptr) {
    // Join
    node->GetJoin()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  } else if (!node->GetList().empty()) {
    // Multiple table
    for (auto &table : node->GetList())
      table->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>());
  } else {
    // Single table
    if (catalog_accessor_->GetTableOid(node->GetTableName()) == catalog::INVALID_TABLE_OID) {
      throw BINDER_EXCEPTION(fmt::format("relation \"{}\" does not exist", node->GetTableName()),
                             common::ErrorCode::ERRCODE_UNDEFINED_TABLE);
    }
    context_->AddRegularTable(catalog_accessor_, node, db_oid_);
  }
}

void BindNodeVisitor::UnifyOrderByExpression(
    common::ManagedPointer<parser::OrderByDescription> order_by_description,
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_items) {
  auto &exprs = order_by_description->GetOrderByExpressions();
  auto size = order_by_description->GetOrderByExpressionsSize();
  for (size_t idx = 0; idx < size; idx++) {
    if (exprs[idx].Get()->GetExpressionType() == terrier::parser::ExpressionType::VALUE_CONSTANT) {
      auto constant_value_expression = exprs[idx].CastManagedPointerTo<parser::ConstantValueExpression>();
      type::TypeId type = constant_value_expression->GetReturnValueType();
      int64_t column_id = 0;
      switch (type) {
        case type::TypeId::TINYINT:
        case type::TypeId::SMALLINT:
        case type::TypeId::INTEGER:
        case type::TypeId::BIGINT:
          column_id = constant_value_expression->GetInteger().val_;
          break;
        case type::TypeId::DECIMAL:
          column_id = constant_value_expression->GetReal().val_;
          break;
        default:
          throw BINDER_EXCEPTION("non-integer constant in ORDER BY", common::ErrorCode::ERRCODE_SYNTAX_ERROR);
      }
      if (column_id < 1 || column_id > static_cast<int64_t>(select_items.size())) {
        throw BINDER_EXCEPTION(fmt::format("ORDER BY position \"{}\" is not in select list", std::to_string(column_id)),
                               common::ErrorCode::ERRCODE_UNDEFINED_COLUMN);
      }
      exprs[idx] = select_items[column_id - 1];
    }
  }
}
}  // namespace terrier::binder
