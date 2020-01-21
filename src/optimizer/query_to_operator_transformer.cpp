#include "optimizer/query_to_operator_transformer.h"

#include <algorithm>
#include <cmath>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "loggers/optimizer_logger.h"
#include "optimizer/logical_operators.h"
#include "optimizer/operator_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/subquery_expression.h"
#include "parser/expression_util.h"
#include "parser/postgresparser.h"
#include "parser/statements.h"
#include "planner/plannodes/plan_node_defs.h"

namespace terrier::optimizer {

QueryToOperatorTransformer::QueryToOperatorTransformer(
    const common::ManagedPointer<catalog::CatalogAccessor> catalog_accessor)
    : accessor_(catalog_accessor) {
  output_expr_ = nullptr;
}

std::unique_ptr<OperatorExpression> QueryToOperatorTransformer::ConvertToOpExpression(
    common::ManagedPointer<parser::SQLStatement> op, parser::ParseResult *parse_result) {
  output_expr_ = nullptr;
  op->Accept(this, parse_result);
  return std::move(output_expr_);
}

void QueryToOperatorTransformer::Visit(parser::SelectStatement *op, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming SelectStatement to operators ...");
  // We do not visit the select list of a base table because the column
  // information is derived before the plan generation, at this step we
  // don't need to derive that
  auto pre_predicates = std::move(predicates_);
  predicates_ = {};

  if (op->GetSelectTable() != nullptr) {
    // SELECT with FROM
    op->GetSelectTable()->Accept(this, parse_result);
  } else {
    // SELECT without FROM
    output_expr_ =
        std::make_unique<OperatorExpression>(LogicalGet::Make(), std::vector<std::unique_ptr<OperatorExpression>>{});
  }

  if (op->GetSelectCondition() != nullptr) {
    OPTIMIZER_LOG_DEBUG("Collecting predicates ...");
    CollectPredicates(op->GetSelectCondition(), parse_result, &predicates_);
  }

  if (!predicates_.empty()) {
    auto filter_expr = std::make_unique<OperatorExpression>(LogicalFilter::Make(std::move(predicates_)),
                                                            std::vector<std::unique_ptr<OperatorExpression>>{});
    filter_expr->PushChild(std::move(output_expr_));
    predicates_.clear();
    output_expr_ = std::move(filter_expr);
  }

  if (QueryToOperatorTransformer::RequireAggregation(common::ManagedPointer(op))) {
    OPTIMIZER_LOG_DEBUG("Handling aggregation in SelectStatement ...");
    // Plain aggregation
    std::unique_ptr<OperatorExpression> agg_expr;
    if (op->GetSelectGroupBy() == nullptr) {
      // TODO(boweic): aggregation without groupby could still have having clause
      agg_expr = std::make_unique<OperatorExpression>(LogicalAggregateAndGroupBy::Make(),
                                                      std::vector<std::unique_ptr<OperatorExpression>>{});
      agg_expr->PushChild(std::move(output_expr_));
      output_expr_ = std::move(agg_expr);
    } else {
      size_t num_group_by_cols = op->GetSelectGroupBy()->GetColumns().size();
      auto group_by_cols = std::vector<common::ManagedPointer<parser::AbstractExpression>>(num_group_by_cols);
      for (size_t i = 0; i < num_group_by_cols; i++) {
        group_by_cols[i] = common::ManagedPointer<parser::AbstractExpression>(op->GetSelectGroupBy()->GetColumns()[i]);
      }
      agg_expr = std::make_unique<OperatorExpression>(LogicalAggregateAndGroupBy::Make(std::move(group_by_cols)),
                                                      std::vector<std::unique_ptr<OperatorExpression>>{});
      agg_expr->PushChild(std::move(output_expr_));
      output_expr_ = std::move(agg_expr);

      std::vector<AnnotatedExpression> having;
      if (op->GetSelectGroupBy()->GetHaving() != nullptr) {
        CollectPredicates(op->GetSelectGroupBy()->GetHaving(), parse_result, &having);
      }
      if (!having.empty()) {
        auto filter_expr = std::make_unique<OperatorExpression>(LogicalFilter::Make(std::move(having)),
                                                                std::vector<std::unique_ptr<OperatorExpression>>{});
        filter_expr->PushChild(std::move(output_expr_));
        output_expr_ = std::move(filter_expr);
      }
    }
  } else if (op->IsSelectDistinct()) {
    // SELECT DISTINCT a1 FROM A should be transformed to
    // SELECT a1 FROM A GROUP BY a1
    auto num_cols = op->GetSelectColumns().size();
    auto group_by_cols = std::vector<common::ManagedPointer<parser::AbstractExpression>>(num_cols);
    for (size_t i = 0; i < num_cols; i++) {
      group_by_cols[i] = common::ManagedPointer<parser::AbstractExpression>(op->GetSelectColumns()[i]);
    }

    std::vector<std::unique_ptr<OperatorExpression>> c;
    c.emplace_back(std::move(output_expr_));
    output_expr_ =
        std::make_unique<OperatorExpression>(LogicalAggregateAndGroupBy::Make(std::move(group_by_cols)), std::move(c));
  }

  if (op->GetSelectLimit() != nullptr && op->GetSelectLimit()->GetLimit() != -1) {
    OPTIMIZER_LOG_DEBUG("Handling order by/limit/offset in SelectStatement ...");
    std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_exprs;
    std::vector<optimizer::OrderByOrderingType> sort_direction;

    if (op->GetSelectOrderBy() != nullptr) {
      const auto &order_info = op->GetSelectOrderBy();
      for (auto &expr : order_info->GetOrderByExpressions()) {
        sort_exprs.push_back(expr);
      }
      for (auto &type : order_info->GetOrderByTypes()) {
        if (type == parser::kOrderAsc)
          sort_direction.push_back(optimizer::OrderByOrderingType::ASC);
        else
          sort_direction.push_back(optimizer::OrderByOrderingType::DESC);
      }
    }
    auto limit_expr = std::make_unique<OperatorExpression>(
        LogicalLimit::Make(op->GetSelectLimit()->GetOffset(), op->GetSelectLimit()->GetLimit(), std::move(sort_exprs),
                           std::move(sort_direction)),
        std::vector<std::unique_ptr<OperatorExpression>>{});
    limit_expr->PushChild(std::move(output_expr_));
    output_expr_ = std::move(limit_expr);
  }

  predicates_ = std::move(pre_predicates);
}

void QueryToOperatorTransformer::Visit(parser::JoinDefinition *node, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming JoinDefinition to operators ...");
  // Get left operator
  node->GetLeftTable()->Accept(this, parse_result);
  auto left_expr = std::move(output_expr_);

  // Get right operator
  node->GetRightTable()->Accept(this, parse_result);
  auto right_expr = std::move(output_expr_);

  // Construct join operator
  std::unique_ptr<OperatorExpression> join_expr;
  std::vector<AnnotatedExpression> join_predicates;
  CollectPredicates(node->GetJoinCondition(), parse_result, &join_predicates);
  switch (node->GetJoinType()) {
    case parser::JoinType::INNER: {
      join_expr = std::make_unique<OperatorExpression>(LogicalInnerJoin::Make(std::move(join_predicates)),
                                                       std::vector<std::unique_ptr<OperatorExpression>>{});
      join_expr->PushChild(std::move(left_expr));
      join_expr->PushChild(std::move(right_expr));
      break;
    }
    case parser::JoinType::OUTER: {
      join_expr = std::make_unique<OperatorExpression>(LogicalOuterJoin::Make(std::move(join_predicates)),
                                                       std::vector<std::unique_ptr<OperatorExpression>>{});
      join_expr->PushChild(std::move(left_expr));
      join_expr->PushChild(std::move(right_expr));
      break;
    }
    case parser::JoinType::LEFT: {
      join_expr = std::make_unique<OperatorExpression>(LogicalLeftJoin::Make(std::move(join_predicates)),
                                                       std::vector<std::unique_ptr<OperatorExpression>>{});
      join_expr->PushChild(std::move(left_expr));
      join_expr->PushChild(std::move(right_expr));
      break;
    }
    case parser::JoinType::RIGHT: {
      join_expr = std::make_unique<OperatorExpression>(LogicalRightJoin::Make(std::move(join_predicates)),
                                                       std::vector<std::unique_ptr<OperatorExpression>>{});
      join_expr->PushChild(std::move(left_expr));
      join_expr->PushChild(std::move(right_expr));
      break;
    }
    case parser::JoinType::SEMI: {
      join_expr = std::make_unique<OperatorExpression>(LogicalSemiJoin::Make(std::move(join_predicates)),
                                                       std::vector<std::unique_ptr<OperatorExpression>>{});
      join_expr->PushChild(std::move(left_expr));
      join_expr->PushChild(std::move(right_expr));
      break;
    }
    default:
      throw OPTIMIZER_EXCEPTION("Join type invalid");
  }

  output_expr_ = std::move(join_expr);
}

void QueryToOperatorTransformer::Visit(parser::TableRef *node, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming TableRef to operators ...");
  if (node->GetSelect() != nullptr) {
    // Store previous context

    // Construct query derived table predicates
    // i.e. the mapping from column name to the underlying expression in the sub-query.
    // This is needed to generate input/output information for subqueries
    auto table_alias = node->GetAlias();
    std::transform(table_alias.begin(), table_alias.end(), table_alias.begin(), ::tolower);

    auto alias_to_expr_map = ConstructSelectElementMap(node->GetSelect()->GetSelectColumns());

    node->GetSelect()->Accept(this, parse_result);

    auto child_expr = std::move(output_expr_);
    output_expr_ =
        std::make_unique<OperatorExpression>(LogicalQueryDerivedGet::Make(table_alias, std::move(alias_to_expr_map)),
                                             std::vector<std::unique_ptr<OperatorExpression>>{});
    output_expr_->PushChild(std::move(child_expr));
  } else if (node->GetJoin() != nullptr) {
    // Explicit Join
    node->GetJoin()->Accept(this, parse_result);
  } else if (node->GetList().size() > 1) {
    // Multiple tables (Implicit Join)
    // Create a join operator between the first two tables
    node->GetList().at(0)->Accept(this, parse_result);
    auto prev_expr = std::move(output_expr_);
    // Build a left deep join tree
    for (size_t i = 1; i < node->GetList().size(); i++) {
      // Start at i = 1 due to the Accept() above
      auto list_elem = node->GetList().at(i);
      list_elem->Accept(this, parse_result);
      auto join_expr = std::make_unique<OperatorExpression>(LogicalInnerJoin::Make(),
                                                            std::vector<std::unique_ptr<OperatorExpression>>{});
      join_expr->PushChild(std::move(prev_expr));
      join_expr->PushChild(std::move(output_expr_));
      TERRIER_ASSERT(join_expr->GetChildren().size() == 2, "The join expr should have exactly 2 elements");
      prev_expr = std::move(join_expr);
    }
    output_expr_ = std::move(prev_expr);
  } else {
    // Single table
    if (node->GetList().size() == 1) node = node->GetList().at(0).Get();

    // TODO(Ling): how should we determine the value of `is_for_update` field of logicalGet constructor?
    output_expr_ = std::make_unique<OperatorExpression>(
        LogicalGet::Make(accessor_->GetDatabaseOid(node->GetDatabaseName()), accessor_->GetDefaultNamespace(),
                         accessor_->GetTableOid(node->GetTableName()), {}, node->GetAlias(), false),
        std::vector<std::unique_ptr<OperatorExpression>>{});
  }
}

void QueryToOperatorTransformer::Visit(parser::GroupByDescription *node, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming GroupByDescription to operators ...");
}
void QueryToOperatorTransformer::Visit(parser::OrderByDescription *node, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming OrderByDescription to operators ...");
}
void QueryToOperatorTransformer::Visit(UNUSED_ATTRIBUTE parser::LimitDescription *node,
                                       UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming LimitDescription to operators ...");
}
void QueryToOperatorTransformer::Visit(parser::CreateFunctionStatement *op,
                                       UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming CreateFunctionStatement to operators ...");
  // TODO(Ling): Where should the as_type_ go?
  std::vector<std::string> function_param_names;
  std::vector<parser::BaseFunctionParameter::DataType> function_param_types;
  for (const auto &col : op->GetFuncParameters()) {
    function_param_names.push_back(col->GetParamName());
    function_param_types.push_back(col->GetDataType());
  }
  // TODO(Ling): database oid of create function?
  auto create_expr = std::make_unique<OperatorExpression>(
      LogicalCreateFunction::Make(catalog::INVALID_DATABASE_OID, accessor_->GetDefaultNamespace(), op->GetFuncName(),
                                  op->GetPLType(), op->GetFuncBody(), std::move(function_param_names),
                                  std::move(function_param_types), op->GetFuncReturnType()->GetDataType(),
                                  op->GetFuncParameters().size(), op->ShouldReplace()),
      std::vector<std::unique_ptr<OperatorExpression>>{});
  output_expr_ = std::move(create_expr);
}

void QueryToOperatorTransformer::Visit(parser::CreateStatement *op, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming CreateStatement to operators ...");
  auto create_type = op->GetCreateType();
  std::unique_ptr<OperatorExpression> create_expr;
  switch (create_type) {
    case parser::CreateStatement::CreateType::kDatabase:
      create_expr = std::make_unique<OperatorExpression>(LogicalCreateDatabase::Make(op->GetDatabaseName()),
                                                         std::vector<std::unique_ptr<OperatorExpression>>{});
      break;
    case parser::CreateStatement::CreateType::kTable:
      create_expr = std::make_unique<OperatorExpression>(
          LogicalCreateTable::Make(accessor_->GetDefaultNamespace(), op->GetTableName(), op->GetColumns(),
                                   op->GetForeignKeys()),
          std::vector<std::unique_ptr<OperatorExpression>>{});
      // TODO(Ling): for other procedures to generate create table plan, refer to create_table_plan_node builder.
      //  Following part might be more adequate to be handled by optimizer when it it actually constructing the plan
      //  I don't think we should extract out the desired fields here.
      break;
    case parser::CreateStatement::CreateType::kIndex: {
      // create vector of expressions of the index entires
      std::vector<common::ManagedPointer<parser::AbstractExpression>> entries;
      for (auto &attr : op->GetIndexAttributes()) {
        if (attr.HasExpr()) {
          entries.emplace_back(attr.GetExpression());
        } else {
          // TODO(Matt): can an index attribute definition ever reference multiple tables? I don't think so. We should
          // probably move this out of the loop.
          // TODO(Matt): why are we still binding names to oids at this point? Didn't the binder already bind this
          // expression?
          const auto tb_oid = accessor_->GetTableOid(op->GetTableName());
          const auto &table_schema = accessor_->GetSchema(tb_oid);
          const auto &table_col = table_schema.GetColumn(attr.GetName());
          auto unique_col_expr = std::make_unique<parser::ColumnValueExpression>(
              op->GetTableName(), attr.GetName(), accessor_->GetDatabaseOid(op->GetDatabaseName()), tb_oid,
              table_col.Oid(), table_col.Type());
          parse_result->AddExpression(std::move(unique_col_expr));
          auto new_col_expr = common::ManagedPointer(parse_result->GetExpressions().back());
          entries.push_back(new_col_expr);
        }
      }
      create_expr = std::make_unique<OperatorExpression>(
          LogicalCreateIndex::Make(accessor_->GetDefaultNamespace(), accessor_->GetTableOid(op->GetTableName()),
                                   op->GetIndexType(), op->IsUniqueIndex(), op->GetIndexName(), std::move(entries)),
          std::vector<std::unique_ptr<OperatorExpression>>{});
      break;
    }
    case parser::CreateStatement::CreateType::kTrigger: {
      std::vector<catalog::col_oid_t> trigger_columns;
      auto tb_oid = accessor_->GetTableOid(op->GetTableName());
      auto schema = accessor_->GetSchema(tb_oid);
      for (const auto &col : op->GetTriggerColumns()) trigger_columns.emplace_back(schema.GetColumn(col).Oid());
      create_expr = std::make_unique<OperatorExpression>(
          LogicalCreateTrigger::Make(accessor_->GetDatabaseOid(op->GetDatabaseName()), accessor_->GetDefaultNamespace(),
                                     tb_oid, op->GetTriggerName(), op->GetTriggerFuncNames(), op->GetTriggerArgs(),
                                     std::move(trigger_columns), op->GetTriggerWhen(), op->GetTriggerType()),
          std::vector<std::unique_ptr<OperatorExpression>>{});
      break;
    }
    case parser::CreateStatement::CreateType::kSchema:
      create_expr = std::make_unique<OperatorExpression>(LogicalCreateNamespace::Make(op->GetNamespaceName()),
                                                         std::vector<std::unique_ptr<OperatorExpression>>{});
      break;
    case parser::CreateStatement::CreateType::kView:
      create_expr = std::make_unique<OperatorExpression>(
          LogicalCreateView::Make(accessor_->GetDatabaseOid(op->GetDatabaseName()), accessor_->GetDefaultNamespace(),
                                  op->GetViewName(), op->GetViewQuery()),
          std::vector<std::unique_ptr<OperatorExpression>>{});
      break;
  }

  output_expr_ = std::move(create_expr);
}
void QueryToOperatorTransformer::Visit(parser::InsertStatement *op, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming InsertStatement to operators ...");
  auto target_table = op->GetInsertionTable();
  auto target_table_id = accessor_->GetTableOid(target_table->GetTableName());
  auto target_db_id = accessor_->GetDatabaseOid(target_table->GetDatabaseName());
  auto target_ns_id = accessor_->GetDefaultNamespace();

  if (op->GetInsertType() == parser::InsertType::SELECT) {
    auto insert_expr =
        std::make_unique<OperatorExpression>(LogicalInsertSelect::Make(target_db_id, target_ns_id, target_table_id),
                                             std::vector<std::unique_ptr<OperatorExpression>>{});
    op->GetSelect()->Accept(this, parse_result);
    insert_expr->PushChild(std::move(output_expr_));
    output_expr_ = std::move(insert_expr);
    return;
  }

  // column_objects represents the columns for the current table as defined in its schema
  auto column_objects = accessor_->GetSchema(target_table_id).GetColumns();

  // vector of column oids
  std::vector<catalog::col_oid_t> col_ids;

  // INSERT INTO table_name VALUES (val1, val2, ...), (val_a, val_b, ...), ...
  if (op->GetInsertColumns()->empty()) {
    for (const auto &values : *(op->GetValues())) {
      if (values.size() > column_objects.size()) {
        throw CATALOG_EXCEPTION("INSERT has more expressions than target columns");
      }
      if (values.size() < column_objects.size()) {
        for (auto i = values.size(); i != column_objects.size(); ++i) {
          // check whether null values or default values can be used in the rest of the columns
          if (!column_objects[i].Nullable() && column_objects[i].StoredExpression() == nullptr) {
            throw CATALOG_EXCEPTION(
                ("Null value in column \"" + column_objects[i].Name() + "\" violates not-null constraint").c_str());
          }
        }
      }
    }
    for (const auto &col : column_objects) col_ids.push_back(col.Oid());

  } else {
    // INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...), ...
    auto num_columns = op->GetInsertColumns()->size();
    for (const auto &tuple : *(op->GetValues())) {  // check size of each tuple
      if (tuple.size() > num_columns) {
        throw CATALOG_EXCEPTION("INSERT has more expressions than target columns");
      }
      if (tuple.size() < num_columns) {
        throw CATALOG_EXCEPTION("INSERT has more target columns than expressions");
      }
    }

    // The set below contains names of columns mentioned in the insert statement
    std::unordered_set<catalog::col_oid_t> specified;
    auto schema = accessor_->GetSchema(target_table_id);

    for (const auto &col : *(op->GetInsertColumns())) {
      try {
        const auto &column_object = schema.GetColumn(col);
        specified.insert(column_object.Oid());
        col_ids.emplace_back(column_object.Oid());
      } catch (const std::out_of_range &oor) {
        throw CATALOG_EXCEPTION(
            ("Column \"" + col + "\" of relation \"" + target_table->GetTableName() + "\" does not exist").c_str());
      }
    }

    for (const auto &column : schema.GetColumns()) {
      // this loop checks not null constraint for unspecified columns
      if (specified.find(column.Oid()) == specified.end() && !column.Nullable() &&
          column.StoredExpression() == nullptr) {
        throw CATALOG_EXCEPTION(
            ("Null value in column \"" + column.Name() + "\" violates not-null constraint").c_str());
      }
    }
  }

  auto insert_expr = std::make_unique<OperatorExpression>(
      LogicalInsert::Make(target_db_id, target_ns_id, target_table_id, std::move(col_ids), op->GetValues()),
      std::vector<std::unique_ptr<OperatorExpression>>{});
  output_expr_ = std::move(insert_expr);
}

void QueryToOperatorTransformer::Visit(parser::DeleteStatement *op, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming DeleteStatement to operators ...");
  auto target_table = op->GetDeletionTable();
  auto target_db_id = accessor_->GetDatabaseOid(target_table->GetDatabaseName());
  auto target_table_id = accessor_->GetTableOid(target_table->GetTableName());
  auto target_ns_id = accessor_->GetDefaultNamespace();
  auto target_table_alias = target_table->GetAlias();

  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto delete_expr = std::make_unique<OperatorExpression>(
      LogicalDelete::Make(target_db_id, target_ns_id, target_table_alias, target_table_id), std::move(c));

  std::unique_ptr<OperatorExpression> table_scan;
  if (op->GetDeleteCondition() != nullptr) {
    std::vector<AnnotatedExpression> predicates;
    QueryToOperatorTransformer::ExtractPredicates(op->GetDeleteCondition(), &predicates);
    table_scan = std::make_unique<OperatorExpression>(
        LogicalGet::Make(target_db_id, target_ns_id, target_table_id, predicates, target_table_alias, true),
        std::vector<std::unique_ptr<OperatorExpression>>{});
  } else {
    table_scan = std::make_unique<OperatorExpression>(
        LogicalGet::Make(target_db_id, target_ns_id, target_table_id, {}, target_table_alias, true),
        std::vector<std::unique_ptr<OperatorExpression>>{});
  }
  delete_expr->PushChild(std::move(table_scan));

  output_expr_ = std::move(delete_expr);
}

void QueryToOperatorTransformer::Visit(parser::DropStatement *op, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming DropStatement to operators ...")
  auto drop_type = op->GetDropType();
  std::unique_ptr<OperatorExpression> drop_expr;
  switch (drop_type) {
    case parser::DropStatement::DropType::kDatabase:
      drop_expr = std::make_unique<OperatorExpression>(
          LogicalDropDatabase::Make(accessor_->GetDatabaseOid(op->GetDatabaseName())),
          std::vector<std::unique_ptr<OperatorExpression>>{});
      break;
    case parser::DropStatement::DropType::kTable:
      drop_expr =
          std::make_unique<OperatorExpression>(LogicalDropTable::Make(accessor_->GetTableOid(op->GetTableName())),
                                               std::vector<std::unique_ptr<OperatorExpression>>{});
      break;
    case parser::DropStatement::DropType::kIndex:
      drop_expr =
          std::make_unique<OperatorExpression>(LogicalDropIndex::Make(accessor_->GetIndexOid(op->GetIndexName())),
                                               std::vector<std::unique_ptr<OperatorExpression>>{});
      break;
    case parser::DropStatement::DropType::kSchema:
      drop_expr = std::make_unique<OperatorExpression>(
          LogicalDropNamespace::Make(accessor_->GetNamespaceOid(op->GetNamespaceName())),
          std::vector<std::unique_ptr<OperatorExpression>>{});
      break;
    case parser::DropStatement::DropType::kTrigger:
    case parser::DropStatement::DropType::kView:
    case parser::DropStatement::DropType::kPreparedStatement:
      break;
  }

  output_expr_ = std::move(drop_expr);
}
void QueryToOperatorTransformer::Visit(UNUSED_ATTRIBUTE parser::PrepareStatement *op,
                                       UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming PrepareStatement to operators ...");
}
void QueryToOperatorTransformer::Visit(UNUSED_ATTRIBUTE parser::ExecuteStatement *op,
                                       UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming ExecuteStatement to operators ...");
}
void QueryToOperatorTransformer::Visit(UNUSED_ATTRIBUTE parser::TransactionStatement *op,
                                       UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming Transaction to operators ...");
}

void QueryToOperatorTransformer::Visit(parser::UpdateStatement *op,
                                       UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming UpdateStatement to operators ...");
  auto target_table = op->GetUpdateTable();
  auto target_db_id = accessor_->GetDatabaseOid(target_table->GetDatabaseName());
  auto target_table_id = accessor_->GetTableOid(target_table->GetTableName());
  auto target_ns_id = accessor_->GetDefaultNamespace();
  auto target_table_alias = target_table->GetAlias();

  std::unique_ptr<OperatorExpression> table_scan;

  std::vector<std::unique_ptr<OperatorExpression>> c;
  auto update_expr = std::make_unique<OperatorExpression>(
      LogicalUpdate::Make(target_db_id, target_ns_id, target_table_alias, target_table_id, op->GetUpdateClauses()),
      std::move(c));

  if (op->GetUpdateCondition() != nullptr) {
    std::vector<AnnotatedExpression> predicates;
    QueryToOperatorTransformer::ExtractPredicates(op->GetUpdateCondition(), &predicates);
    table_scan = std::make_unique<OperatorExpression>(
        LogicalGet::Make(target_db_id, target_ns_id, target_table_id, predicates, target_table_alias, true),
        std::vector<std::unique_ptr<OperatorExpression>>{});
  } else {
    table_scan = std::make_unique<OperatorExpression>(
        LogicalGet::Make(target_db_id, target_ns_id, target_table_id, {}, target_table_alias, true),
        std::vector<std::unique_ptr<OperatorExpression>>{});
  }
  update_expr->PushChild(std::move(table_scan));

  output_expr_ = std::move(update_expr);
}

void QueryToOperatorTransformer::Visit(parser::CopyStatement *op, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming CopyStatement to operators ...");
  if (op->IsFrom()) {
    // The copy statement is reading from a file into a table. We construct a
    // logical external-file get operator as the leaf, and an insert operator
    // as the root.
    auto get_op = std::make_unique<OperatorExpression>(
        LogicalExternalFileGet::Make(op->GetExternalFileFormat(), op->GetFilePath(), op->GetDelimiter(),
                                     op->GetQuoteChar(), op->GetEscapeChar()),
        std::vector<std::unique_ptr<OperatorExpression>>{});

    auto target_table = op->GetCopyTable();

    auto insert_op = std::make_unique<OperatorExpression>(
        LogicalInsertSelect::Make(accessor_->GetDatabaseOid(target_table->GetDatabaseName()),
                                  accessor_->GetDefaultNamespace(),
                                  accessor_->GetTableOid(target_table->GetTableName())),
        std::vector<std::unique_ptr<OperatorExpression>>{});
    insert_op->PushChild(std::move(get_op));
    output_expr_ = std::move(insert_op);

  } else {
    if (op->GetSelectStatement() != nullptr) {
      op->GetSelectStatement()->Accept(this, parse_result);
    } else {
      op->GetCopyTable()->Accept(this, parse_result);
    }
    auto export_op = std::make_unique<OperatorExpression>(
        LogicalExportExternalFile::Make(op->GetExternalFileFormat(), op->GetFilePath(), op->GetDelimiter(),
                                        op->GetQuoteChar(), op->GetEscapeChar()),
        std::vector<std::unique_ptr<OperatorExpression>>{});
    export_op->PushChild(std::move(output_expr_));

    output_expr_ = std::move(export_op);
  }
}

void QueryToOperatorTransformer::Visit(UNUSED_ATTRIBUTE parser::AnalyzeStatement *op,
                                       UNUSED_ATTRIBUTE parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming AnalyzeStatement to operators ...");
}

void QueryToOperatorTransformer::Visit(parser::ComparisonExpression *expr, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming ComparisonExpression to operators ...");
  auto expr_type = expr->GetExpressionType();
  if (expr->GetExpressionType() == parser::ExpressionType::COMPARE_IN) {
    GenerateSubqueryTree(expr, 1, parse_result, false);
  } else if (expr_type == parser::ExpressionType::COMPARE_EQUAL ||
             expr_type == parser::ExpressionType::COMPARE_GREATER_THAN ||
             expr_type == parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO ||
             expr_type == parser::ExpressionType::COMPARE_LESS_THAN ||
             expr_type == parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO) {
    if (expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::ROW_SUBQUERY &&
        expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::ROW_SUBQUERY) {
      throw NOT_IMPLEMENTED_EXCEPTION("Comparisons between sub-selects are not supported");
    }
    // Transform if either child is sub-query
    GenerateSubqueryTree(expr, 0, parse_result, true) || GenerateSubqueryTree(expr, 1, parse_result, true);
  }
  expr->AcceptChildren(this, parse_result);
}

void QueryToOperatorTransformer::Visit(parser::OperatorExpression *expr, parser::ParseResult *parse_result) {
  OPTIMIZER_LOG_DEBUG("Transforming OperatorExpression to operators ...");
  // TODO(boweic): We may want to do the rewrite (exist -> in) in the binder
  if (expr->GetExpressionType() == parser::ExpressionType::OPERATOR_EXISTS) {
    if (GenerateSubqueryTree(expr, 0, parse_result, false)) {
      // Already reset the child to column, we need to transform exist to not-null to preserve semantic
      expr->SetExpressionType(parser::ExpressionType::OPERATOR_IS_NOT_NULL);
    }
  }

  expr->AcceptChildren(this, parse_result);
}

bool QueryToOperatorTransformer::RequireAggregation(common::ManagedPointer<parser::SelectStatement> op) {
  if (op->GetSelectGroupBy() != nullptr) {
    return true;
  }
  // Check plain aggregation
  bool has_aggregation = false;
  bool has_other_exprs = false;

  for (auto &expr : op->GetSelectColumns()) {
    std::vector<common::ManagedPointer<parser::AggregateExpression>> aggr_exprs;
    // we need to use get method of managed pointer because the function we are calling will recursivly get aggreate
    // expressions from the current expression and its children; children are of unique pointers
    parser::ExpressionUtil::GetAggregateExprs(&aggr_exprs, expr);
    if (!aggr_exprs.empty())
      has_aggregation = true;
    else
      has_other_exprs = true;
  }
  // TODO(peloton): Should be handled in the binder
  // Syntax error when there are mixture of aggregation and other exprs when group by is absent
  if (has_aggregation && has_other_exprs) {
    throw OPTIMIZER_EXCEPTION(
        "Non aggregation expression must appear in the GROUP BY clause or be used in an aggregate function");
  }
  return has_aggregation;
}

void QueryToOperatorTransformer::CollectPredicates(common::ManagedPointer<parser::AbstractExpression> expr,
                                                   parser::ParseResult *parse_result,
                                                   std::vector<AnnotatedExpression> *predicates) {
  // First check if all conjunctive predicates are supported before transforming
  // predicate with sub-select into regular predicates
  std::vector<common::ManagedPointer<parser::AbstractExpression>> predicate_ptrs;
  QueryToOperatorTransformer::SplitPredicates(expr, &predicate_ptrs);
  for (const auto &pred : predicate_ptrs) {
    if (!QueryToOperatorTransformer::IsSupportedConjunctivePredicate(pred)) {
      throw NOT_IMPLEMENTED_EXCEPTION(
          ("Expression type " + std::to_string(static_cast<int>(pred.Get()->GetExpressionType())) + " is not supported")
              .c_str());
    }
  }
  // Accept will change the expression, e.g. (a in (select b from test)) into
  // (a IN test.b), after the rewrite, we can extract the table aliases
  // information correctly
  expr->Accept(this, parse_result);
  QueryToOperatorTransformer::ExtractPredicates(expr, predicates);
}

bool QueryToOperatorTransformer::IsSupportedConjunctivePredicate(
    common::ManagedPointer<parser::AbstractExpression> expr) {
  // Currently support : 1. expr without subquery
  // 2. subquery without disjunction. Since the expr is already one of the
  // conjunctive exprs, we'll only need to check if the root level is an
  // operator with subquery
  if (!expr->HasSubquery()) {
    return true;
  }
  auto expr_type = expr->GetExpressionType();
  // Subquery with IN
  if (expr_type == parser::ExpressionType::COMPARE_IN &&
      expr->GetChild(0)->GetExpressionType() != parser::ExpressionType::ROW_SUBQUERY &&
      expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::ROW_SUBQUERY) {
    return true;
  }
  // Subquery with EXIST
  if (expr_type == parser::ExpressionType::OPERATOR_EXISTS &&
      expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::ROW_SUBQUERY) {
    return true;
  }
  // Subquery with other operator
  if (expr_type == parser::ExpressionType::COMPARE_EQUAL || expr_type == parser::ExpressionType::COMPARE_GREATER_THAN ||
      expr_type == parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO ||
      expr_type == parser::ExpressionType::COMPARE_LESS_THAN ||
      expr_type == parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO) {
    // Supported if one child is subquery and the other is not
    if ((!expr->GetChild(0)->HasSubquery() &&
         expr->GetChild(1)->GetExpressionType() == parser::ExpressionType::ROW_SUBQUERY) ||
        (!expr->GetChild(1)->HasSubquery() &&
         expr->GetChild(0)->GetExpressionType() == parser::ExpressionType::ROW_SUBQUERY)) {
      return true;
    }
  }
  return false;
}

bool QueryToOperatorTransformer::IsSupportedSubSelect(common::ManagedPointer<parser::SelectStatement> op) {
  // Supported if 1. No aggregation. 2. With aggregation and WHERE clause only
  // have correlated columns in conjunctive predicates in the form of
  // "outer_relation.a = ..."
  // TODO(boweic): Add support for arbitary expressions, this would require
  //  the support for mark join & some special operators, see Hyper's unnesting arbitary query slides
  if (!QueryToOperatorTransformer::RequireAggregation(op)) return true;

  std::vector<common::ManagedPointer<parser::AbstractExpression>> predicates;
  QueryToOperatorTransformer::SplitPredicates(op->GetSelectCondition(), &predicates);
  for (const auto &pred : predicates) {
    // Depth is used to detect correlated subquery, it is set in the binder,
    // if a TVE has depth less than the depth of the current operator, then it is correlated predicate
    if (pred->GetDepth() < op->GetDepth()) {
      if (pred->GetExpressionType() != parser::ExpressionType::COMPARE_EQUAL) {
        return false;
      }
      // Check if in the form of "outer_relation.a = (expr with only columns in inner relation)"
      if (!((pred->GetChild(1)->GetDepth() == op->GetDepth() &&
             pred->GetChild(0)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) ||
            (pred->GetChild(0)->GetDepth() == op->GetDepth() &&
             pred->GetChild(1)->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE))) {
        return false;
      }
    }
  }
  return true;
}

bool QueryToOperatorTransformer::GenerateSubqueryTree(parser::AbstractExpression *expr, int child_id,
                                                      parser::ParseResult *parse_result, bool single_join) {
  // Get potential subquery
  auto subquery_expr = expr->GetChild(child_id);
  if (subquery_expr->GetExpressionType() != parser::ExpressionType::ROW_SUBQUERY) return false;

  auto sub_select = subquery_expr.CastManagedPointerTo<parser::SubqueryExpression>()->GetSubselect();
  if (!QueryToOperatorTransformer::IsSupportedSubSelect(sub_select))
    throw NOT_IMPLEMENTED_EXCEPTION("Sub-select not supported");
  // We only support subselect with single row
  if (sub_select->GetSelectColumns().size() != 1) throw NOT_IMPLEMENTED_EXCEPTION("Array in predicates not supported");

  std::vector<parser::AbstractExpression *> select_list;
  // Construct join
  std::unique_ptr<OperatorExpression> op_expr;
  if (single_join) {
    op_expr = std::make_unique<OperatorExpression>(LogicalSingleJoin::Make(),
                                                   std::vector<std::unique_ptr<OperatorExpression>>{});
    op_expr->PushChild(std::move(output_expr_));
  } else {
    op_expr = std::make_unique<OperatorExpression>(LogicalMarkJoin::Make(),
                                                   std::vector<std::unique_ptr<OperatorExpression>>{});
    op_expr->PushChild(std::move(output_expr_));
  }

  sub_select->Accept(this, parse_result);

  // Push subquery output
  op_expr->PushChild(std::move(output_expr_));

  output_expr_ = std::move(op_expr);

  // Convert subquery to the selected column in the sub-select
  expr->SetChild(child_id, sub_select->GetSelectColumns().at(0));
  return true;
}

void QueryToOperatorTransformer::ExtractPredicates(common::ManagedPointer<parser::AbstractExpression> expr,
                                                   std::vector<AnnotatedExpression> *annotated_predicates) {
  // Split a complex predicate into a set of predicates connected by AND.
  std::vector<common::ManagedPointer<parser::AbstractExpression>> predicates;
  QueryToOperatorTransformer::SplitPredicates(expr, &predicates);

  for (auto predicate : predicates) {
    std::unordered_set<std::string> table_alias_set;
    QueryToOperatorTransformer::GenerateTableAliasSet(predicate, &table_alias_set);

    // Deep copy expression to avoid memory leak
    annotated_predicates->push_back(AnnotatedExpression(predicate, std::move(table_alias_set)));
  }
}

void QueryToOperatorTransformer::GenerateTableAliasSet(const common::ManagedPointer<parser::AbstractExpression> expr,
                                                       std::unordered_set<std::string> *table_alias_set) {
  if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
    table_alias_set->insert(expr.CastManagedPointerTo<const parser::ColumnValueExpression>()->GetTableName());
  } else {
    for (const auto &child : expr->GetChildren())
      QueryToOperatorTransformer::GenerateTableAliasSet(child, table_alias_set);
  }
}

void QueryToOperatorTransformer::SplitPredicates(
    common::ManagedPointer<parser::AbstractExpression> expr,
    std::vector<common::ManagedPointer<parser::AbstractExpression>> *predicates) {
  if (expr == nullptr) {
    return;
  }

  if (expr->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND) {
    // Traverse down the expression tree along conjunction
    for (const auto &child : expr->GetChildren()) {
      QueryToOperatorTransformer::SplitPredicates(common::ManagedPointer(child), predicates);
    }
  } else {
    // Find an expression that is the child of conjunction expression
    predicates->push_back(expr);
  }
}

std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>
QueryToOperatorTransformer::ConstructSelectElementMap(
    const std::vector<common::ManagedPointer<parser::AbstractExpression>> &select_list) {
  std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>> res;
  for (auto &expr : select_list) {
    std::string alias;
    if (!expr->GetAlias().empty()) {
      alias = expr->GetAlias();
    } else if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      auto tv_expr = expr.CastManagedPointerTo<parser::ColumnValueExpression>();
      alias = tv_expr->GetColumnName();
    } else {
      continue;
    }
    std::transform(alias.begin(), alias.end(), alias.begin(), ::tolower);
    res[alias] = common::ManagedPointer<parser::AbstractExpression>(expr);
  }
  return res;
}

}  // namespace terrier::optimizer
