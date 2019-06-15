#include "parser/expression_util.h"
#include "optimizer/input_column_deriver.h"
#include "optimizer/memo.h"
#include "optimizer/operator_expression.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "storage/data_table.h"

namespace terrier {
namespace optimizer {

/**
 * Trivial constructor
 */
InputColumnDeriver::InputColumnDeriver() = default;

/**
 * Derives the input and output columns for a physical operator
 * @param gexpr Group Expression to derive for
 * @param properties Relevant data properties
 * @param required_cols Vector of required output columns
 * @param memo Memo
 * @returns pair where first element is the output columns and the second
 *          element is a vector of inputs from each child.
 *
 * Pointers returned are not ManagedPointer. However, the returned pointers
 * should not be deleted or ever modified.
 */
std::pair<std::vector<const parser::AbstractExpression*>,
          std::vector<std::vector<const parser::AbstractExpression*>>>
InputColumnDeriver::DeriveInputColumns(GroupExpression *gexpr, PropertySet* properties,
                                       std::vector<const parser::AbstractExpression*> required_cols,
                                       Memo *memo) {
  properties_ = properties;
  gexpr_ = gexpr;
  required_cols_ = move(required_cols);
  memo_ = memo;
  gexpr->Op().Accept(this);
  return move(output_input_cols_);
}

/**
 * Derive input and output columns for TableFreeScan (none exist)
 */
void InputColumnDeriver::Visit(const TableFreeScan *) {}

/**
 * Derive input and output columns for SeqScan.
 * Invokes ScanHelper
 */
void InputColumnDeriver::Visit(const SeqScan *) { ScanHelper(); }

/**
 * Derive input and output columns for IndexScan.
 * Invokes ScanHelper
 */
void InputColumnDeriver::Visit(const IndexScan *) { ScanHelper(); }

/**
 * Derive input and output columns for ExternalFileScan.
 * Invokes ScanHelper
 */
void InputColumnDeriver::Visit(const ExternalFileScan *) { ScanHelper(); }

/**
 * Derive input and output columns for QueryDerivedScan.
 * QueryDerivedScan should only be a renaming layer which means the output columns
 * are all the TupleValueExpressions in required_cols_ and the input columns
 * are the columns specified in QueryDerivedScan->alias_to_expr_map.
 */
void InputColumnDeriver::Visit(const QueryDerivedScan *op) {
  // QueryDerivedScan should only be a renaming layer
  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    parser::ExpressionUtil::GetTupleValueExprs(output_cols_map, expr);
  }

  auto output_cols = std::vector<const parser::AbstractExpression*>(output_cols_map.size());
  std::vector<const parser::AbstractExpression*> input_cols(output_cols.size());
  for (auto &entry : output_cols_map) {
    auto tv_expr = dynamic_cast<const parser::TupleValueExpression*>(entry.first);
    TERRIER_ASSERT(tv_expr, "GetTupleValueExprs should only find TupleValueExpressions");
    output_cols[entry.second] = tv_expr;

    // Get the actual expression
    auto map = op->GetAliasToExprMap();
    auto managed_col = map[tv_expr->GetColumnName()];
    const parser::AbstractExpression *input_col = managed_col.operator->();

    // QueryDerivedScan only modify the column name to be a tv_expr, does not change the mapping
    input_cols[entry.second] = input_col;
  }

  output_input_cols_ = std::make_pair(output_cols, std::vector{input_cols});
}

/**
 * Derive input and output columns for Limit.
 * For the physical limit, the input and output columns are identical.
 * The columns include the sort columns, any TupleValueExpressions, and
 * any AggregateExpressions in required_cols_.
 */
void InputColumnDeriver::Visit(const Limit *op) {
  // All aggregate expressions and TVEs in the required columns and internal 
  // sort columns are needed by the child node
  ExprSet input_cols_set;
  for (auto expr : required_cols_) {
    if (parser::ExpressionUtil::IsAggregateExpression(expr)) {
      input_cols_set.insert(expr);
    } else {
      parser::ExpressionUtil::GetTupleValueExprs(input_cols_set, expr);
    }
  }

  auto sort_expressions = op->GetSortExpressions();
  for (const auto& sort_column : sort_expressions) {
    input_cols_set.insert(sort_column.get());
  }

  std::vector<const parser::AbstractExpression*> cols;
  for (const auto &expr : input_cols_set) {
    cols.push_back(expr);
  }

  output_input_cols_ = std::make_pair(cols, std::vector{cols});
}

/**
 * Derives the input and output columns for OrderBy
 * The output columns and input[0] columns are identical.
 * The columns include all AggregateExpressions and TupleValueExpressions from
 * required_cols_ and the sort columns.
 */
void InputColumnDeriver::Visit(const OrderBy *) {
  // we need to pass down both required columns and sort columns
  auto prop = properties_->GetPropertyOfType(PropertyType::SORT);
  TERRIER_ASSERT(prop != nullptr, "property should exist");

  ExprSet input_cols_set;
  for (auto expr : required_cols_) {
    if (parser::ExpressionUtil::IsAggregateExpression(expr)) {
      input_cols_set.insert(expr);
    } else {
      parser::ExpressionUtil::GetTupleValueExprs(input_cols_set, expr);
    }
  }

  auto sort_prop = prop->As<PropertySort>();
  size_t sort_col_size = sort_prop->GetSortColumnSize();
  for (size_t idx = 0; idx < sort_col_size; ++idx) {
    input_cols_set.insert(sort_prop->GetSortColumn(idx).get());
  }

  std::vector<const parser::AbstractExpression*> cols;
  for (auto &expr : input_cols_set) {
    cols.push_back(expr);
  }

  output_input_cols_ = std::make_pair(cols, std::vector{cols});
}

/**
 * Derives the input and output columns for HashGroupBy
 * Relies on AggregateHelper
 */
void InputColumnDeriver::Visit(const HashGroupBy *op) {
  AggregateHelper(op);
}

/**
 * Derives the input and output columns for SortGroupBy
 * Relies on AggregateHelper
 */
void InputColumnDeriver::Visit(const SortGroupBy *op) {
  AggregateHelper(op);
}

/**
 * Derives the input and output columns for Aggregate
 * Relies on AggregateHelper
 */
void InputColumnDeriver::Visit(const Aggregate *op) {
  AggregateHelper(op);
}

/**
 * Derives the input and output columns for Distinct
 * Relies on Passdown()
 */
void InputColumnDeriver::Visit(const Distinct *) {
  Passdown();
}

/**
 * Derives the input and output columns for InnerNLJoin
 * Relies on JoinHelper
 */
void InputColumnDeriver::Visit(const InnerNLJoin *op) {
  JoinHelper(op);
}

/**
 * Derives the input and output columns for LeftNLJoin
 * Currently not supported.
 */
void InputColumnDeriver::Visit(const LeftNLJoin *) {
  TERRIER_ASSERT(0, "LeftNLJoin not supported");
}

/**
 * Derives the input and output columns for RightNLJoin
 * Currently not supported.
 */
void InputColumnDeriver::Visit(const RightNLJoin *) {
  TERRIER_ASSERT(0, "RightNLJoin not supported");
}

/**
 * Derives the input and output columns for OuterNLJoin
 * Currently not supported.
 */
void InputColumnDeriver::Visit(const OuterNLJoin *) {
  TERRIER_ASSERT(0, "OuterNLJoin not supported");
}

/**
 * Derives the input and output columns for InnerHashJoin
 * Relies on JoinHelper
 */
void InputColumnDeriver::Visit(const InnerHashJoin *op) {
  JoinHelper(op);
}

/**
 * Derives the input and output columns for LeftHashJoin
 * Currently not supported.
 */
void InputColumnDeriver::Visit(const LeftHashJoin *) {
  TERRIER_ASSERT(0, "LeftHashJoin not supported");
}

/**
 * Derives the input and output columns for RightHashJoin
 * Currently not supported.
 */
void InputColumnDeriver::Visit(const RightHashJoin *) {
  TERRIER_ASSERT(0, "RightHashJoin not supported");
}

/**
 * Derives the input and output columns for OuterHashJoin
 * Currently not supported.
 */
void InputColumnDeriver::Visit(const OuterHashJoin *) {
  TERRIER_ASSERT(0, "OuterHashJoin not supported");
}

/**
 * Derives input and output columns for a Insert.
 *  Insert has no input columns and all output columns are the required_cols_
 */
void InputColumnDeriver::Visit(const Insert *) {
  auto input = std::vector<std::vector<const parser::AbstractExpression*>>{};
  output_input_cols_ = std::make_pair(required_cols_, input);
}

/**
 * Derives input and output columns for InsertSelect.
 * Relies on Passdown()
 */
void InputColumnDeriver::Visit(const InsertSelect *) {
  Passdown();
}

/**
 * Derives input and output columns for Delete.
 * Relies on Passdown()
 */
void InputColumnDeriver::Visit(const Delete *) {
  Passdown();
}

/**
 * Derives input and output columns for Update.
 * Relies on Passdown()
 */
void InputColumnDeriver::Visit(const Update *) {
  Passdown();
}

/**
 * Derives input and output columns for ExportExternalFile.
 * Relies on Passdown()
 */
void InputColumnDeriver::Visit(const ExportExternalFile *) {
  Passdown();
}

/**
 * Helper to derive the output columns of a scan operator.
 * A scan operator has no input columns, and the output columns are the set
 * of all columns required (i.e required_cols_).
 */
void InputColumnDeriver::ScanHelper() {
  // Derive all output columns from required_cols_.
  // Since Scan are "lowest" level, only care about TupleValueExpression in required_cols_
  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    parser::ExpressionUtil::GetTupleValueExprs(output_cols_map, expr);
  }

  auto output_cols = std::vector<const parser::AbstractExpression*>(output_cols_map.size());
  for (auto &entry : output_cols_map) {
    output_cols[entry.second] = entry.first;
  }

  // Scan does not have input columns
  auto input = std::vector<std::vector<const parser::AbstractExpression*>>{};
  output_input_cols_ = std::make_pair(output_cols, input);
}

/**
 * Derive all input and output columns for an Aggregate
 * The output columns are all Tuple and Aggregation columns from required_cols_
 * and any extra columns used by having expressions.
 *
 * The input column vector contains of a single vector consisting of all
 * TupleValueExpressions needed by GroupBy and Having expressions and any
 * TupleValueExpressions required by AggregateExpression from required_cols_.
 */
void InputColumnDeriver::AggregateHelper(const BaseOperatorNode *op) {
  ExprSet input_cols_set;
  ExprMap output_cols_map;
  unsigned int output_col_idx = 0;
  for (size_t idx = 0; idx < required_cols_.size(); ++idx) {
    auto &expr = required_cols_[idx];

    // Get all AggregateExpressions and TupleValueExpressions in expr
    std::vector<const parser::AggregateExpression*> aggr_exprs;
    std::vector<const parser::TupleValueExpression*> tv_exprs;
    parser::ExpressionUtil::GetTupleAndAggregateExprs(aggr_exprs, tv_exprs, expr);

    for (auto &aggr_expr : aggr_exprs) {
      if (!output_cols_map.count(aggr_expr)) {
        // Add this AggregateExpression to output column since it is an output
        output_cols_map[aggr_expr] = output_col_idx++;
        size_t child_size = aggr_expr->GetChildrenSize();
        for (size_t idx = 0; idx < child_size; ++idx) {
          // Add all TupleValueExpression used by the Aggregate to input columns
          parser::ExpressionUtil::GetTupleValueExprs(input_cols_set, aggr_expr->GetChild(idx));
        }
      }
    }

    // TV expr not in aggregation (must be in groupby, so we do not need to add to input columns)
    for (auto &tv_expr : tv_exprs) {
      if (!output_cols_map.count(tv_expr)) {
        output_cols_map[tv_expr] = output_col_idx++;
      }
      // input_cols_set.insert(tv_expr);
    }
  }

  std::vector<common::ManagedPointer<parser::AbstractExpression>> groupby_cols;
  std::vector<AnnotatedExpression> having_exprs;
  if (op->GetType() == OpType::HASHGROUPBY) {
    auto groupby = reinterpret_cast<const HashGroupBy *>(op);
    groupby_cols = groupby->GetColumns();
    having_exprs = groupby->GetHaving();
  } else if (op->GetType() == OpType::SORTGROUPBY) {
    auto groupby = reinterpret_cast<const SortGroupBy *>(op);
    groupby_cols = groupby->GetColumns();
    having_exprs = groupby->GetHaving();
  }

  // Add all group by columns to the list of input columns
  for (auto &groupby_col : groupby_cols) {
    input_cols_set.insert(groupby_col.get());
  }

  // Check having predicate, since the predicate may use columns other than output columns
  for (auto &having_expr : having_exprs) {
    // We perform aggregate here so the output contains aggregate exprs while
    // input should contain all tuple value exprs used to perform aggregation.
    parser::ExpressionUtil::GetTupleValueExprs(input_cols_set, having_expr.GetExpr().get());
    parser::ExpressionUtil::GetTupleAndAggregateExprs(output_cols_map, having_expr.GetExpr().get());
  }

  // Create the input_cols vector
  std::vector<const parser::AbstractExpression*> input_cols;
  for (auto &col : input_cols_set) {
    input_cols.push_back(col);
  }

  // Create the output_cols vector
  output_col_idx = static_cast<unsigned int>(output_cols_map.size());
  std::vector<const parser::AbstractExpression*> output_cols(output_col_idx);
  for (auto &expr_idx_pair : output_cols_map) {
    output_cols[expr_idx_pair.second] = expr_idx_pair.first;
  }

  output_input_cols_ = std::make_pair(output_cols, std::vector{input_cols});
}

/**
 * Derives the output and input columns for a Join.
 *
 * The output columns are all the TupleValueExpression and AggregateExpressions
 * as located in required_cols_. The set of all input columns are built by consolidating
 * all TupleValueExpression and AggregateExpression in all left_keys/right_keys/join_conds
 * and any in required_cols_ are are split as input_cols = {build_cols, probe_cols}
 * based on build-side table aliases and probe-side table aliases.
 *
 * NOTE:
 * - This function assumes the build side is the Left Child
 * - This function assumes the probe side is the Right Child
 * TODO(wz2): Better abstraction/identification of build/probe rather than hard-coded
 */
void InputColumnDeriver::JoinHelper(const BaseOperatorNode *op) {
  std::vector<AnnotatedExpression> join_conds;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> left_keys;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> right_keys;
  if (op->GetType() == OpType::INNERHASHJOIN) {
    auto join_op = reinterpret_cast<const InnerHashJoin *>(op);
    join_conds = join_op->GetJoinPredicates();
    left_keys = join_op->GetLeftKeys();
    right_keys = join_op->GetRightKeys();
  } else if (op->GetType() == OpType::INNERNLJOIN) {
    auto join_op = reinterpret_cast<const InnerNLJoin *>(op);
    join_conds = join_op->GetJoinPredicates();
    left_keys = join_op->GetLeftKeys();
    right_keys = join_op->GetRightKeys();
  }

  ExprSet input_cols_set;
  for (auto &left_key : left_keys) {
    // Get all Tuple/Aggregate expressions from left keys
    parser::ExpressionUtil::GetTupleAndAggregateExprs(input_cols_set, left_key.get());
  }
  for (auto &right_key : right_keys) {
    // Get all Tuple/Aggregate expressions from right keys
    parser::ExpressionUtil::GetTupleAndAggregateExprs(input_cols_set, right_key.get());
  }
  for (auto &join_cond : join_conds) {
    // Get all Tuple/Aggregate expressions from join conditions
    parser::ExpressionUtil::GetTupleAndAggregateExprs(input_cols_set, join_cond.GetExpr().get());
  }

  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    parser::ExpressionUtil::GetTupleAndAggregateExprs(output_cols_map, expr);
  }
  for (auto &expr_idx_pair : output_cols_map) {
    input_cols_set.insert(expr_idx_pair.first);
  }

  // Construct input columns
  ExprSet build_table_cols_set;
  ExprSet probe_table_cols_set;
  auto &build_table_aliases = memo_->GetGroupByID(gexpr_->GetChildGroupId(0))->GetTableAliases();
  auto &probe_table_aliases = memo_->GetGroupByID(gexpr_->GetChildGroupId(1))->GetTableAliases();
  for (auto &col : input_cols_set) {
    const parser::TupleValueExpression* tv_expr;
    if (col->GetExpressionType() == parser::ExpressionType::VALUE_TUPLE) {
      tv_expr = dynamic_cast<const parser::TupleValueExpression *>(col);
      TERRIER_ASSERT(tv_expr, "col should be a TupleValueExpression");
    } else {
      TERRIER_ASSERT(parser::ExpressionUtil::IsAggregateExpression(col), "col should be AggregateExpression");

      ExprSet tv_exprs;
      // Get the TupleValueExpression used in the AggregateExpression
      parser::ExpressionUtil::GetTupleValueExprs(tv_exprs, col);
      if (tv_exprs.empty()) {
        // Do not need input columns like COUNT(1)
        continue;
      }

      // We get only the first TupleValueExpression (should probably assert check this)
      tv_expr = dynamic_cast<const parser::TupleValueExpression*>(*(tv_exprs.begin()));
      TERRIER_ASSERT(tv_expr, "GetTupleValueExprs should only return TupleValueExpression");
      TERRIER_ASSERT(tv_exprs.size() == 1, "Uh oh, multiple TVEs in AggregateExpression found");
    }

    // Pick the build or probe side depending on the table
    std::string tv_table_name = tv_expr->GetTableName();
    if (build_table_aliases.count(tv_table_name)) {
      build_table_cols_set.insert(col);
    } else {
      TERRIER_ASSERT(probe_table_aliases.count(tv_table_name), "tv_expr should be against probe table");
      probe_table_cols_set.insert(col);
    }
  }

  // Derive output columns
  std::vector<const parser::AbstractExpression*> output_cols(output_cols_map.size());
  for (auto &expr_idx_pair : output_cols_map) {
    output_cols[expr_idx_pair.second] = expr_idx_pair.first;
  }

  // Derive build columns (first element of input column vector)
  std::vector<const parser::AbstractExpression*> build_cols;
  for (auto &col : build_table_cols_set) {
    build_cols.push_back(col);
  }

  // Derive probe columns (second element of input column vector)
  std::vector<const parser::AbstractExpression*> probe_cols;
  for (auto &col : probe_table_cols_set) {
    probe_cols.push_back(col);
  }

  output_input_cols_ = std::make_pair(output_cols, std::vector{build_cols, probe_cols});
}

/**
 * Passes down the list of required columns as input columns
 * Sets output_input_cols_ = (required_cols, {required_cols_})
 */
void InputColumnDeriver::Passdown() {
  output_input_cols_ = std::make_pair(required_cols_, std::vector{required_cols_});
}

}  // namespace optimizer
}  // namespace terrier
