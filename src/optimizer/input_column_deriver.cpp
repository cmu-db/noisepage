#include "optimizer/input_column_deriver.h"

#include <string>
#include <utility>
#include <vector>

#include "optimizer/memo.h"
#include "optimizer/operator_node.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "optimizer/util.h"
#include "parser/expression_util.h"
#include "storage/data_table.h"

namespace noisepage::optimizer {

/**
 * Definition for first type of pair
 */
using PT1 = std::vector<common::ManagedPointer<parser::AbstractExpression>>;

/**
 * Definition for second type of pair
 * This is defined here to make following code more readable and
 * also to satisfy clang-tidy.
 */
using PT2 = std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>;

std::pair<PT1, PT2> InputColumnDeriver::DeriveInputColumns(
    GroupExpression *gexpr, PropertySet *properties,
    std::vector<common::ManagedPointer<parser::AbstractExpression>> required_cols, Memo *memo) {
  properties_ = properties;
  gexpr_ = gexpr;
  required_cols_ = std::move(required_cols);
  memo_ = memo;
  gexpr->Contents()->Accept(common::ManagedPointer<OperatorVisitor>(this));
  return std::move(output_input_cols_);
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const TableFreeScan *op) {}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const SeqScan *) { ScanHelper(); }

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const IndexScan *) { ScanHelper(); }

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const ExternalFileScan *) { ScanHelper(); }

void InputColumnDeriver::Visit(const QueryDerivedScan *op) {
  // QueryDerivedScan should only be a renaming layer
  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    parser::ExpressionUtil::GetTupleValueExprs(&output_cols_map, expr);
  }

  auto output_cols = std::vector<common::ManagedPointer<parser::AbstractExpression>>(output_cols_map.size());
  std::vector<common::ManagedPointer<parser::AbstractExpression>> input_cols(output_cols.size());
  auto alias_expr_map = op->GetAliasToExprMap();
  for (auto &entry : output_cols_map) {
    auto tv_expr = entry.first.CastManagedPointerTo<parser::ColumnValueExpression>();
    output_cols[entry.second] = entry.first;

    // Get the actual expression
    auto input_col = alias_expr_map[tv_expr->GetColumnName()];

    // QueryDerivedScan only modify the column name to be a tv_expr, does not change the mapping
    input_cols[entry.second] = input_col;
  }

  PT2 child_cols = PT2{input_cols};
  output_input_cols_ = std::make_pair(std::move(output_cols), std::move(child_cols));
}

void InputColumnDeriver::Visit(const Limit *op) {
  // All aggregate expressions and TVEs in the required columns and internal
  // sort columns are needed by the child node
  ExprSet input_cols_set;
  for (auto expr : required_cols_) {
    if (parser::ExpressionUtil::IsAggregateExpression(expr)) {
      input_cols_set.insert(expr);
    } else {
      parser::ExpressionUtil::GetTupleValueExprs(&input_cols_set, expr);
    }
  }

  auto sort_expressions = op->GetSortExpressions();
  for (const auto &sort_column : sort_expressions) {
    input_cols_set.insert(sort_column);
  }

  std::vector<common::ManagedPointer<parser::AbstractExpression>> cols;
  for (const auto &expr : input_cols_set) {
    cols.push_back(expr);
  }

  PT2 child_cols = PT2{cols};
  output_input_cols_ = std::make_pair(std::move(cols), std::move(child_cols));
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const OrderBy *op) {
  // we need to pass down both required columns and sort columns
  auto prop = properties_->GetPropertyOfType(PropertyType::SORT);
  NOISEPAGE_ASSERT(prop != nullptr, "property should exist");

  ExprSet input_cols_set;
  for (auto expr : required_cols_) {
    if (parser::ExpressionUtil::IsAggregateExpression(expr)) {
      input_cols_set.insert(expr);
    } else {
      parser::ExpressionUtil::GetTupleValueExprs(&input_cols_set, expr);
    }
  }

  auto sort_prop = prop->As<PropertySort>();
  size_t sort_col_size = sort_prop->GetSortColumnSize();
  for (size_t idx = 0; idx < sort_col_size; ++idx) {
    input_cols_set.insert(sort_prop->GetSortColumn(idx));
  }

  std::vector<common::ManagedPointer<parser::AbstractExpression>> cols;
  for (auto &expr : input_cols_set) {
    cols.push_back(expr);
  }

  PT2 child_cols = PT2{cols};
  output_input_cols_ = std::make_pair(std::move(cols), std::move(child_cols));
}

void InputColumnDeriver::Visit(const HashGroupBy *op) { AggregateHelper(op); }

void InputColumnDeriver::Visit(const SortGroupBy *op) { AggregateHelper(op); }

void InputColumnDeriver::Visit(const Aggregate *op) { AggregateHelper(op); }

void InputColumnDeriver::Visit(const InnerIndexJoin *op) {
  ExprSet input_cols_set;
  for (auto &join_keys : op->GetJoinKeys()) {
    // Get all Tuple/Aggregate expressions from join keys
    for (auto join_key : join_keys.second) {
      if (join_key != nullptr) {
        parser::ExpressionUtil::GetTupleAndAggregateExprs(&input_cols_set, join_key);
      }
    }
  }
  for (auto &join_cond : op->GetJoinPredicates()) {
    // Get all Tuple/Aggregate expressions from join conditions
    parser::ExpressionUtil::GetTupleAndAggregateExprs(&input_cols_set, join_cond.GetExpr());
  }

  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    parser::ExpressionUtil::GetTupleAndAggregateExprs(&output_cols_map, expr);
  }
  for (auto &expr_idx_pair : output_cols_map) {
    input_cols_set.insert(expr_idx_pair.first);
  }

  // Construct input columns
  ExprSet probe_table_cols_set;
  auto &probe_table_aliases = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetTableAliases();
  for (auto &col : input_cols_set) {
    common::ManagedPointer<parser::ColumnValueExpression> tv_expr;
    if (col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      tv_expr = col.CastManagedPointerTo<parser::ColumnValueExpression>();
    } else {
      NOISEPAGE_ASSERT(parser::ExpressionUtil::IsAggregateExpression(col), "col should be AggregateExpression");

      ExprSet tv_exprs;
      // Get the ColumnValueExpression used in the AggregateExpression
      parser::ExpressionUtil::GetTupleValueExprs(&tv_exprs, col);
      if (tv_exprs.empty()) {
        // Do not need input columns like COUNT(1)
        continue;
      }

      // We get only the first ColumnValueExpression (should probably assert check this)
      tv_expr = (*(tv_exprs.begin())).CastManagedPointerTo<parser::ColumnValueExpression>();
      NOISEPAGE_ASSERT(tv_exprs.size() == 1, "Uh oh, multiple TVEs in AggregateExpression found");
    }

    // Pick the probe if alias matches
    std::string tv_table_name = tv_expr->GetTableName();
    NOISEPAGE_ASSERT(!tv_table_name.empty(), "Table Name should not be empty");
    if (probe_table_aliases.count(tv_table_name) != 0U) {
      probe_table_cols_set.insert(col);
    }
  }

  // Derive output columns
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols(output_cols_map.size());
  for (auto &expr_idx_pair : output_cols_map) {
    output_cols[expr_idx_pair.second] = expr_idx_pair.first;
  }

  std::vector<common::ManagedPointer<parser::AbstractExpression>> probe_cols;
  for (auto &col : probe_table_cols_set) {
    probe_cols.push_back(col);
  }

  PT2 child_cols = PT2{probe_cols};
  output_input_cols_ = std::make_pair(std::move(output_cols), std::move(child_cols));
}

void InputColumnDeriver::Visit(const InnerNLJoin *op) { JoinHelper(op); }

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const LeftNLJoin *op) {
  NOISEPAGE_ASSERT(0, "LeftNLJoin not supported");
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const RightNLJoin *op) {
  NOISEPAGE_ASSERT(0, "RightNLJoin not supported");
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const OuterNLJoin *op) {
  NOISEPAGE_ASSERT(0, "OuterNLJoin not supported");
}

void InputColumnDeriver::Visit(const InnerHashJoin *op) { JoinHelper(op); }

void InputColumnDeriver::Visit(const LeftSemiHashJoin *op) { JoinHelper(op); }

void InputColumnDeriver::Visit(const LeftHashJoin *op) { JoinHelper(op); }

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const RightHashJoin *op) {
  NOISEPAGE_ASSERT(0, "RightHashJoin not supported");
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const OuterHashJoin *op) {
  NOISEPAGE_ASSERT(0, "OuterHashJoin not supported");
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const Insert *op) {
  auto input = std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>{};
  output_input_cols_ = std::make_pair(std::move(required_cols_), std::move(input));
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const InsertSelect *op) { Passdown(); }

void InputColumnDeriver::InputBaseTableColumns(const std::string &alias, catalog::db_oid_t db,
                                               catalog::table_oid_t tbl) {
  auto exprs = OptimizerUtil::GenerateTableColumnValueExprs(accessor_, alias, db, tbl);

  std::vector<common::ManagedPointer<parser::AbstractExpression>> inputs(required_cols_);
  for (auto *expr : exprs) {
    inputs.emplace_back(expr);
    txn_->RegisterCommitAction([=]() { delete expr; });
    txn_->RegisterAbortAction([=]() { delete expr; });
  }

  auto input = std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>{std::move(inputs)};
  output_input_cols_ = std::make_pair(std::move(required_cols_), std::move(input));
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const Delete *op) {
  const auto &alias = op->GetTableAlias();
  auto db_id = op->GetDatabaseOid();
  auto tbl_id = op->GetTableOid();
  InputBaseTableColumns(alias, db_id, tbl_id);
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const Update *op) {
  const auto &alias = op->GetTableAlias();
  auto db_id = op->GetDatabaseOid();
  auto tbl_id = op->GetTableOid();
  InputBaseTableColumns(alias, db_id, tbl_id);
}

void InputColumnDeriver::Visit(UNUSED_ATTRIBUTE const ExportExternalFile *op) { Passdown(); }

void InputColumnDeriver::ScanHelper() {
  // Derive all output columns from required_cols_.
  // Since Scan are "lowest" level, only care about ColumnValueExpression in required_cols_
  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    parser::ExpressionUtil::GetTupleValueExprs(&output_cols_map, expr);
  }

  auto output_cols = std::vector<common::ManagedPointer<parser::AbstractExpression>>(output_cols_map.size());
  for (auto &entry : output_cols_map) {
    output_cols[entry.second] = entry.first;
  }

  // Scan does not have input columns
  auto input = std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>{};
  output_input_cols_ = std::make_pair(std::move(output_cols), std::move(input));
}

void InputColumnDeriver::AggregateHelper(const BaseOperatorNodeContents *op) {
  ExprSet input_cols_set;
  ExprMap output_cols_map;
  unsigned int output_col_idx = 0;
  for (auto &expr : required_cols_) {
    // Get all AggregateExpressions and ColumnValueExpressions in expr
    std::vector<common::ManagedPointer<parser::AbstractExpression>> aggr_exprs;
    std::vector<common::ManagedPointer<parser::AbstractExpression>> tv_exprs;
    parser::ExpressionUtil::GetTupleAndAggregateExprs(&aggr_exprs, &tv_exprs, expr);

    for (auto &aggr_expr : aggr_exprs) {
      if (output_cols_map.count(aggr_expr) == 0U) {
        // Add this AggregateExpression to output column since it is an output
        output_cols_map[aggr_expr] = output_col_idx++;
        size_t child_size = aggr_expr->GetChildrenSize();
        for (size_t idx = 0; idx < child_size; ++idx) {
          // Add all ColumnValueExpression used by the Aggregate to input columns
          parser::ExpressionUtil::GetTupleValueExprs(&input_cols_set, aggr_expr->GetChild(idx));
        }
      }
    }

    // TV expr not in aggregation (must be in groupby, so we do not need to add to input columns)
    for (auto &tv_expr : tv_exprs) {
      if (output_cols_map.count(tv_expr) == 0U) {
        output_cols_map[tv_expr] = output_col_idx++;
      }
    }
  }

  std::vector<common::ManagedPointer<parser::AbstractExpression>> groupby_cols;
  std::vector<AnnotatedExpression> having_exprs;
  if (op->GetOpType() == OpType::HASHGROUPBY) {
    auto groupby = reinterpret_cast<const HashGroupBy *>(op);
    groupby_cols = groupby->GetColumns();
    having_exprs = groupby->GetHaving();
  } else if (op->GetOpType() == OpType::SORTGROUPBY) {
    auto groupby = reinterpret_cast<const SortGroupBy *>(op);
    groupby_cols = groupby->GetColumns();
    having_exprs = groupby->GetHaving();
  }

  // Add all group by columns to the list of input columns
  for (auto &groupby_col : groupby_cols) {
    input_cols_set.insert(groupby_col);
  }

  // Check having predicate, since the predicate may use columns other than output columns
  for (auto &having_expr : having_exprs) {
    // We perform aggregate here so the output contains aggregate exprs while
    // input should contain all tuple value exprs used to perform aggregation.
    parser::ExpressionUtil::GetTupleValueExprs(&input_cols_set, having_expr.GetExpr());
    parser::ExpressionUtil::GetTupleAndAggregateExprs(&output_cols_map, having_expr.GetExpr());
  }

  // Create the input_cols vector
  std::vector<common::ManagedPointer<parser::AbstractExpression>> input_cols;
  for (auto &col : input_cols_set) {
    input_cols.push_back(col);
  }

  // Create the output_cols vector
  output_col_idx = static_cast<unsigned int>(output_cols_map.size());
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols(output_col_idx);
  for (auto &expr_idx_pair : output_cols_map) {
    output_cols[expr_idx_pair.second] = expr_idx_pair.first;
  }

  PT2 child_cols = PT2{input_cols};
  output_input_cols_ = std::make_pair(std::move(output_cols), std::move(child_cols));
}

void InputColumnDeriver::JoinHelper(const BaseOperatorNodeContents *op) {
  std::vector<AnnotatedExpression> join_conds;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;
  if (op->GetOpType() == OpType::INNERHASHJOIN) {
    auto join_op = reinterpret_cast<const InnerHashJoin *>(op);
    join_conds = join_op->GetJoinPredicates();
    left_keys = join_op->GetLeftKeys();
    right_keys = join_op->GetRightKeys();
  } else if (op->GetOpType() == OpType::LEFTHASHJOIN) {
    auto join_op = reinterpret_cast<const LeftHashJoin *>(op);
    join_conds = join_op->GetJoinPredicates();
    left_keys = join_op->GetLeftKeys();
    right_keys = join_op->GetRightKeys();
  } else if (op->GetOpType() == OpType::INNERNLJOIN) {
    auto join_op = reinterpret_cast<const InnerNLJoin *>(op);
    join_conds = join_op->GetJoinPredicates();
  } else if (op->GetOpType() == OpType::LEFTSEMIHASHJOIN) {
    auto join_op = reinterpret_cast<const LeftSemiHashJoin *>(op);
    join_conds = join_op->GetJoinPredicates();
    left_keys = join_op->GetLeftKeys();
    right_keys = join_op->GetRightKeys();
  }

  ExprSet input_cols_set;
  for (auto &left_key : left_keys) {
    // Get all Tuple/Aggregate expressions from left keys
    parser::ExpressionUtil::GetTupleAndAggregateExprs(&input_cols_set, left_key);
  }
  for (auto &right_key : right_keys) {
    // Get all Tuple/Aggregate expressions from right keys
    parser::ExpressionUtil::GetTupleAndAggregateExprs(&input_cols_set, right_key);
  }
  for (auto &join_cond : join_conds) {
    // Get all Tuple/Aggregate expressions from join conditions
    parser::ExpressionUtil::GetTupleAndAggregateExprs(&input_cols_set, join_cond.GetExpr());
  }

  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    parser::ExpressionUtil::GetTupleAndAggregateExprs(&output_cols_map, expr);
  }
  for (auto &expr_idx_pair : output_cols_map) {
    input_cols_set.insert(expr_idx_pair.first);
  }

  // Construct input columns
  ExprSet build_table_cols_set;
  ExprSet probe_table_cols_set;
  auto &build_table_aliases = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetTableAliases();
  UNUSED_ATTRIBUTE auto &probe_table_aliases = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetTableAliases();
  for (auto &col : input_cols_set) {
    common::ManagedPointer<parser::ColumnValueExpression> tv_expr;
    if (col->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
      tv_expr = col.CastManagedPointerTo<parser::ColumnValueExpression>();
    } else {
      NOISEPAGE_ASSERT(parser::ExpressionUtil::IsAggregateExpression(col), "col should be AggregateExpression");

      ExprSet tv_exprs;
      // Get the ColumnValueExpression used in the AggregateExpression
      parser::ExpressionUtil::GetTupleValueExprs(&tv_exprs, col);
      if (tv_exprs.empty()) {
        // Do not need input columns like COUNT(1)
        continue;
      }

      // We get only the first ColumnValueExpression (should probably assert check this)
      tv_expr = (*(tv_exprs.begin())).CastManagedPointerTo<parser::ColumnValueExpression>();
      NOISEPAGE_ASSERT(tv_exprs.size() == 1, "Uh oh, multiple TVEs in AggregateExpression found");
    }

    // Pick the build or probe side depending on the table
    std::string tv_table_name = tv_expr->GetTableName();
    NOISEPAGE_ASSERT(!tv_table_name.empty(), "Table Name should not be empty");
    if (build_table_aliases.count(tv_table_name) != 0U) {
      build_table_cols_set.insert(col);
    } else {
      NOISEPAGE_ASSERT(probe_table_aliases.count(tv_table_name), "tv_expr should be against probe table");
      probe_table_cols_set.insert(col);
    }
  }

  // Derive output columns
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output_cols(output_cols_map.size());
  for (auto &expr_idx_pair : output_cols_map) {
    output_cols[expr_idx_pair.second] = expr_idx_pair.first;
  }

  // Derive build columns (first element of input column vector)
  std::vector<common::ManagedPointer<parser::AbstractExpression>> build_cols;
  for (auto &col : build_table_cols_set) {
    build_cols.push_back(col);
  }

  // Derive probe columns (second element of input column vector)
  std::vector<common::ManagedPointer<parser::AbstractExpression>> probe_cols;
  for (auto &col : probe_table_cols_set) {
    probe_cols.push_back(col);
  }

  PT2 child_cols = PT2{build_cols, probe_cols};
  output_input_cols_ = std::make_pair(std::move(output_cols), std::move(child_cols));
}

void InputColumnDeriver::Passdown() {
  auto input = std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>{required_cols_};
  output_input_cols_ = std::make_pair(std::move(required_cols_), std::move(input));
}

}  // namespace noisepage::optimizer
