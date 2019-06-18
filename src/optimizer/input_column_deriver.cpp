#include "parser/expression_util.h"
#include "optimizer/input_column_deriver.h"
#include "optimizer/memo.h"
#include "optimizer/operator_expression.h"
#include "optimizer/physical_operators.h"
#include "optimizer/properties.h"
#include "storage/data_table.h"

namespace terrier {
namespace optimizer {

InputColumnDeriver::InputColumnDeriver() = default;

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

void InputColumnDeriver::Visit(const TableFreeScan *) {}

void InputColumnDeriver::Visit(const SeqScan *) { ScanHelper(); }

void InputColumnDeriver::Visit(const IndexScan *) { ScanHelper(); }

void InputColumnDeriver::Visit(const ExternalFileScan *) { ScanHelper(); }

void InputColumnDeriver::Visit(const QueryDerivedScan *op) {
  // QueryDerivedScan should only be a renaming layer
  ExprMap output_cols_map;
  for (auto expr : required_cols_) {
    parser::ExpressionUtil::GetTupleValueExprs(output_cols_map, expr);
  }

  auto output_cols = std::vector<const parser::AbstractExpression*>(output_cols_map.size());
  std::vector<const parser::AbstractExpression*> input_cols(output_cols.size());
  auto alias_expr_map = op->GetAliasToExprMap();
  for (auto &entry : output_cols_map) {
    auto tv_expr = dynamic_cast<const parser::TupleValueExpression*>(entry.first);
    TERRIER_ASSERT(tv_expr, "GetTupleValueExprs should only find TupleValueExpressions");
    output_cols[entry.second] = tv_expr;

    // Get the actual expression
    const parser::AbstractExpression *input_col = alias_expr_map[tv_expr->GetColumnName()].get();

    // QueryDerivedScan only modify the column name to be a tv_expr, does not change the mapping
    input_cols[entry.second] = input_col;
  }

  output_input_cols_ = std::make_pair(output_cols, std::vector{input_cols});
}

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

void InputColumnDeriver::Visit(const HashGroupBy *op) {
  AggregateHelper(op);
}

void InputColumnDeriver::Visit(const SortGroupBy *op) {
  AggregateHelper(op);
}

void InputColumnDeriver::Visit(const Aggregate *op) {
  AggregateHelper(op);
}

void InputColumnDeriver::Visit(const Distinct *) {
  Passdown();
}

void InputColumnDeriver::Visit(const InnerNLJoin *op) {
  JoinHelper(op);
}

void InputColumnDeriver::Visit(const LeftNLJoin *) {
  TERRIER_ASSERT(0, "LeftNLJoin not supported");
}

void InputColumnDeriver::Visit(const RightNLJoin *) {
  TERRIER_ASSERT(0, "RightNLJoin not supported");
}

void InputColumnDeriver::Visit(const OuterNLJoin *) {
  TERRIER_ASSERT(0, "OuterNLJoin not supported");
}

void InputColumnDeriver::Visit(const InnerHashJoin *op) {
  JoinHelper(op);
}

void InputColumnDeriver::Visit(const LeftHashJoin *) {
  TERRIER_ASSERT(0, "LeftHashJoin not supported");
}

void InputColumnDeriver::Visit(const RightHashJoin *) {
  TERRIER_ASSERT(0, "RightHashJoin not supported");
}

void InputColumnDeriver::Visit(const OuterHashJoin *) {
  TERRIER_ASSERT(0, "OuterHashJoin not supported");
}

void InputColumnDeriver::Visit(const Insert *) {
  auto input = std::vector<std::vector<const parser::AbstractExpression*>>{};
  output_input_cols_ = std::make_pair(required_cols_, input);
}

void InputColumnDeriver::Visit(const InsertSelect *) {
  Passdown();
}

void InputColumnDeriver::Visit(const Delete *) {
  Passdown();
}

void InputColumnDeriver::Visit(const Update *) {
  Passdown();
}

void InputColumnDeriver::Visit(const ExportExternalFile *) {
  Passdown();
}

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
          parser::ExpressionUtil::GetTupleValueExprs(input_cols_set, aggr_expr->GetChild(idx).get());
        }
      }
    }

    // TV expr not in aggregation (must be in groupby, so we do not need to add to input columns)
    for (auto &tv_expr : tv_exprs) {
      if (!output_cols_map.count(tv_expr)) {
        output_cols_map[tv_expr] = output_col_idx++;
      }
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

void InputColumnDeriver::Passdown() {
  output_input_cols_ = std::make_pair(required_cols_, std::vector{required_cols_});
}

}  // namespace optimizer
}  // namespace terrier
