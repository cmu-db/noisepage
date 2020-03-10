#include "binder/bind_node_visitor.h"

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "binder/binder_sherpa.h"
#include "binder/binder_util.h"
#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "common/exception.h"
#include "common/managed_pointer.h"
#include "loggers/binder_logger.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/star_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression/type_cast_expression.h"
#include "parser/sql_statement.h"
#include "type/transient_value_factory.h"

namespace terrier::binder {

BindNodeVisitor::BindNodeVisitor(common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor)
    : catalog_accessor_(catalog_accessor) {}

void BindNodeVisitor::BindNameToNode(common::ManagedPointer<parser::ParseResult> parse_result) {
  // TODO(Matt): something about the number of statements

  BinderSherpa sherpa;
  parse_result->GetStatement(0)->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), common::ManagedPointer(&sherpa));
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::SelectStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting SelectStatement ...");

  BinderContext context(context_);
  context_ = common::ManagedPointer(&context);

  if (node->GetSelectTable() != nullptr) node->GetSelectTable()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);

  if (node->GetSelectCondition() != nullptr) {
    node->GetSelectCondition()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
    node->GetSelectCondition()->DeriveDepth();
    node->GetSelectCondition()->DeriveSubqueryFlag();
  }
  if (node->GetSelectOrderBy() != nullptr) node->GetSelectOrderBy()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);

  if (node->GetSelectLimit() != nullptr) node->GetSelectLimit()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);

  if (node->GetSelectGroupBy() != nullptr) node->GetSelectGroupBy()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);

  std::vector<common::ManagedPointer<parser::AbstractExpression>> new_select_list;
  BINDER_LOG_TRACE("Gathering select columns...");
  for (auto &select_element : node->GetSelectColumns()) {
    if (select_element->GetExpressionType() == parser::ExpressionType::STAR) {
      context_->GenerateAllColumnExpressions(sherpa->GetParseResult(), common::ManagedPointer(&new_select_list));
      continue;
    }

    select_element->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);

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

  context_ = context_->GetUpperContext();
}

// Some sub query nodes inside SelectStatement
void BindNodeVisitor::Visit(common::ManagedPointer<parser::JoinDefinition> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting JoinDefinition ...");
  // The columns in join condition can only bind to the join tables
  node->GetLeftTable()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  node->GetRightTable()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  node->GetJoinCondition()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::TableRef> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting TableRef ...");
  node->TryBindDatabaseName(default_database_name_);
  if (node->GetSelect() != nullptr) {
    if (node->GetAlias().empty()) throw BINDER_EXCEPTION("Alias not found for query derived table");

    // Save the previous context
    auto pre_context = context_;
    node->GetSelect()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
    // TODO(WAN): who exactly should save and restore contexts? Restore the previous level context
    context_ = pre_context;
    // Add the table to the current context at the end
    context_->AddNestedTable(node->GetAlias(), node->GetSelect()->GetSelectColumns());
  } else if (node->GetJoin() != nullptr) {
    // Join
    node->GetJoin()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  } else if (!node->GetList().empty()) {
    // Multiple table
    for (auto &table : node->GetList()) table->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  } else {
    // Single table
    if (catalog_accessor_->GetTableOid(node->GetTableName()) == catalog::INVALID_TABLE_OID) {
      throw BINDER_EXCEPTION("Accessing non-existing table.");
    }
    context_->AddRegularTable(catalog_accessor_, node);
  }
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::GroupByDescription> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting GroupByDescription ...");
  for (auto &col : node->GetColumns()) col->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  if (node->GetHaving() != nullptr) node->GetHaving()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::OrderByDescription> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting OrderByDescription ...");
  for (auto &expr : node->GetOrderByExpressions())
    if (expr != nullptr) expr->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::UpdateStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting UpdateStatement ...");

  TERRIER_ASSERT(context_ == nullptr, "UPDATE should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  auto table_ref = node->GetUpdateTable();
  table_ref->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  if (node->GetUpdateCondition() != nullptr)
    node->GetUpdateCondition()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);

  auto binder_table_data = context_->GetTableMapping(table_ref->GetTableName());
  const auto &table_schema = std::get<2>(*binder_table_data);

  for (auto &update : node->GetUpdateClauses()) {
    auto is_cast_expression = update->GetUpdateValue()->GetExpressionType() == parser::ExpressionType::OPERATOR_CAST;
    auto expected_ret_type = table_schema.GetColumn(update->GetColumnName()).Type();
    auto mismatched_type = expected_ret_type != update->GetUpdateValue()->GetReturnValueType();
    // TODO(WAN): Unfortunately, arbitrary expressions can't be figured out yet and are hard to identify.
    mismatched_type = mismatched_type && update->GetUpdateValue()->GetReturnValueType() != type::TypeId::INVALID;

    if (mismatched_type || is_cast_expression) {
      auto converted = BinderUtil::Convert(update->GetUpdateValue(), expected_ret_type);
      if (converted == nullptr) {
        throw BINDER_EXCEPTION("Conversion cannot be NULL!");
      }
      update->ResetValue(common::ManagedPointer<parser::AbstractExpression>(converted));
      parse_result->AddExpression(std::move(converted));
    }
    update->GetUpdateValue()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::DeleteStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting DeleteStatement ...");

  TERRIER_ASSERT(context_ == nullptr, "DELETE should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  node->GetDeletionTable()->TryBindDatabaseName(default_database_name_);
  auto table = node->GetDeletionTable();
  context_->AddRegularTable(catalog_accessor_, table->GetDatabaseName(), table->GetNamespaceName(),
                            table->GetTableName(), table->GetTableName());

  if (node->GetDeleteCondition() != nullptr) {
    node->GetDeleteCondition()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::LimitDescription *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_TRACE("Visiting LimitDescription ...");
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::CopyStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting CopyStatement ...");

  TERRIER_ASSERT(context_ == nullptr, "COPY should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  if (node->GetCopyTable() != nullptr) {
    node->GetCopyTable()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);

    // If the table is given, we're either writing or reading all columns
    std::vector<common::ManagedPointer<parser::AbstractExpression>> new_select_list;
    context_->GenerateAllColumnExpressions(parse_result, &new_select_list);
    auto col = node->GetSelectStatement()->GetSelectColumns();
    col.insert(std::end(col), std::begin(new_select_list), std::end(new_select_list));
  } else {
    node->GetSelectStatement()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::CreateFunctionStatement *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_TRACE("Visiting CreateFunctionStatement ...");
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::CreateStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting CreateStatement ...");

  TERRIER_ASSERT(context_ == nullptr, "CREATE should be a root (INSERT into CREATE?).");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

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
        if (col->GetDefaultExpression() != nullptr)
          col->GetDefaultExpression()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
        if (col->GetCheckExpression() != nullptr)
          col->GetCheckExpression()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
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
          attr.GetExpression()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
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
      if (node->GetTriggerWhen() != nullptr) node->GetTriggerWhen()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
      break;
    case parser::CreateStatement::CreateType::kSchema:
      // nothing for binder to handler
      break;
    case parser::CreateStatement::CreateType::kView:
      node->TryBindDatabaseName(default_database_name_);
      TERRIER_ASSERT(node->GetViewQuery() != nullptr, "View requires a query");
      node->GetViewQuery()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
      break;
  }

  context_ = context_->GetUpperContext();
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::InsertStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting InsertStatement ...");

  TERRIER_ASSERT(context_ == nullptr, "INSERT should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

  node->GetInsertionTable()->TryBindDatabaseName(default_database_name_);

  // TODO(WAN): It is unclear what this visitor pattern really buys us. Because of the
  //  Visit -> Accept [ -> AcceptChildren] -> Visit loop, we lose the context of what we're currently
  //  binding. Consider binding "INSERT INTO foo VALUES (1, 'a', '2020-01-01'::date);" for input values validation.
  //  The BinderContext can be extended to maintain the table name that we are currently binding, but by the time
  //  the visitor pattern sends you to the actual values, we will have lost track of whether we are currently binding
  //  the first, second, or third column of foo. More generally, the structure of the binder and the expressions it
  //  uses does not seem to allow for easy non-global reasoning. I could also just be missing something?
  //  In any case, this is currently how transforming strings to dates is done.

  auto table = node->GetInsertionTable();
  context_->AddRegularTable(catalog_accessor_, table->GetDatabaseName(), table->GetNamespaceName(),
                            table->GetTableName(), table->GetTableName());

  if (node->GetSelect() != nullptr) {  // INSERT FROM SELECT
    node->GetSelect()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
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
          throw BINDER_EXCEPTION("Insert column does not exist");
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
            throw BINDER_EXCEPTION("Mismatch in number of insert columns and number of insert values.");
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
              auto null_tv = type::TransientValueFactory::GetNull(schema_col.Type());
              auto null_ex = std::make_unique<parser::ConstantValueExpression>(std::move(null_tv));

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
                parse_result->AddExpression(std::move(cur_value));
              } else if (schema_col.Nullable()) {
                // If there is no default value, check if the column is NULLable, meaning we can insert a NULL.
                auto null_ex_mp = common::ManagedPointer(null_ex).CastManagedPointerTo<parser::AbstractExpression>();
                auto pair = std::make_pair(schema_col, null_ex_mp);
                cols.emplace_back(pair);
                // Note that in this case, we must move null_ex as we have taken a managed pointer to it.
                parse_result->AddExpression(std::move(null_ex));
              } else {
                // If none of the above cases could provide a value to be inserted, then we fail.
                throw BINDER_EXCEPTION("Column not present, does not have a default and is non-nullable.");
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

        // Perform input type transformation validation on the schema-ordered values.
        for (size_t i = 0; i < cols.size(); i++) {
          auto ins_col = cols[i].first;
          auto ins_val = cols[i].second;

          auto ret_type = ins_val->GetReturnValueType();
          auto expected_ret_type = ins_col.Type();

          auto is_null = false;
          if (ins_col.Nullable() && ins_val->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT) {
            is_null = ins_val.CastManagedPointerTo<parser::ConstantValueExpression>()->GetValue().Null();
          }
          auto is_cast_expression = ins_val->GetExpressionType() == parser::ExpressionType::OPERATOR_CAST;
          auto mismatched_type = !is_null && ret_type != expected_ret_type;

          // NULL case handled below.
          if (!is_null && (is_cast_expression || mismatched_type)) {
            if (ins_val->GetExpressionType() == parser::ExpressionType::VALUE_DEFAULT) {
              std::unique_ptr<parser::AbstractExpression> temp = ins_col.StoredExpression()->Copy();

              values[i] = common::ManagedPointer(temp);
              parse_result->AddExpression(std::move(temp));
            } else {
              auto converted = BinderUtil::Convert(values[i], expected_ret_type);
              if (converted == nullptr) {
                throw BINDER_EXCEPTION("Conversion cannot be NULL!");
              }
              values[i] = common::ManagedPointer(converted);
              parse_result->AddExpression(std::move(converted));
            }
          }

          // NULL came in as a T_Null by libpg_query, so no type information was associated with it. Fix in binder.
          if (is_null) {
            auto typed_null = type::TransientValueFactory::GetNull(expected_ret_type);
            auto new_expr = std::make_unique<parser::ConstantValueExpression>(std::move(typed_null));
            values[i] = common::ManagedPointer(new_expr).CastManagedPointerTo<parser::AbstractExpression>();
            parse_result->AddExpression(std::move(new_expr));
          }
        }
      }
    }
  }

  context_ = nullptr;
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::FunctionExpression> expr, common::ManagedPointer<BinderSherpa> sherpa) {
  SqlNodeVisitor::Visit(expr, parse_result);

  std::vector<catalog::type_oid_t> arg_types;
  auto children = expr->GetChildren();
  arg_types.reserve(children.size());
  for (const auto &child : children) {
    arg_types.push_back(catalog_accessor_->GetTypeOidFromTypeId(child->GetReturnValueType()));
  }

  auto proc_oid = catalog_accessor_->GetProcOid(expr->GetFuncName(), arg_types);
  if (proc_oid == catalog::INVALID_PROC_OID) {
    throw BINDER_EXCEPTION("Procedure not registered");
  }

  expr->SetProcOid(proc_oid);
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::DropStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting DropStatement ...");

  TERRIER_ASSERT(context_ == nullptr, "DROP should be a root.");
  BinderContext context(nullptr);
  context_ = common::ManagedPointer(&context);

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

  context_ = nullptr;
}
void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::PrepareStatement *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_TRACE("Visiting PrepareStatement ...");
}
void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::ExecuteStatement *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_TRACE("Visiting ExecuteStatement ...");
}
void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::TransactionStatement *node,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_TRACE("Visiting TransactionStatement ...");
}
void BindNodeVisitor::Visit(common::ManagedPointer<parser::AnalyzeStatement> node, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting AnalyzeStatement ...");
  node->GetAnalyzeTable()->TryBindDatabaseName(default_database_name_);
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::ConstantValueExpression *expr,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_TRACE("Visiting ConstantValueExpression ...");
  // TODO(WAN): see comment in Visit(InsertStatement *, ParseResult*)
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE parser::TypeCastExpression *expr,
                            UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  BINDER_LOG_TRACE("Visiting TypeCastExpression...");
  // TODO(WAN): see comment in Visit(InsertStatement *, ParseResult*)
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::ColumnValueExpression> expr, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting ColumnValueExpression ...");
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

void BindNodeVisitor::Visit(common::ManagedPointer<parser::CaseExpression> expr, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting CaseExpression ...");
  for (size_t i = 0; i < expr->GetWhenClauseSize(); ++i) {
    expr->GetWhenClauseCondition(i)->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
  }
}

void BindNodeVisitor::Visit(common::ManagedPointer<parser::SubqueryExpression> expr, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting SubqueryExpression ...");
  expr->GetSubselect()->Accept(common::ManagedPointer(this).CastManagedPointerTo<SqlNodeVisitor>(), sherpa);
}

void BindNodeVisitor::Visit(UNUSED_ATTRIBUTE common::ManagedPointer<parser::StarExpression> expr,
                            UNUSED_ATTRIBUTE common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting StarExpression ...");
  if (context_ == nullptr || !context_->HasTables()) {
    throw BINDER_EXCEPTION("Invalid [Expression :: STAR].");
  }
}

// Derive value type for these expressions
void BindNodeVisitor::Visit(common::ManagedPointer<parser::OperatorExpression> expr, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting OperatorExpression ...");
  SqlNodeVisitor::Visit(expr, sherpa);
  expr->DeriveReturnValueType();
}
void BindNodeVisitor::Visit(common::ManagedPointer<parser::AggregateExpression> expr, common::ManagedPointer<BinderSherpa> sherpa) {
  BINDER_LOG_TRACE("Visiting AggregateExpression ...");
  SqlNodeVisitor::Visit(expr, sherpa);
  expr->DeriveReturnValueType();
}
}  // namespace terrier::binder
