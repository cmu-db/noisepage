#pragma once

#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "loggers/optimizer_logger.h"
#include "optimizer/optimizer_defs.h"
#include "parser/expression/aggregate_expression.h"
#include "parser/expression/case_expression.h"
#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/derived_value_expression.h"
#include "parser/expression/function_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/expression/type_cast_expression.h"

namespace noisepage::parser {

/**
 * Collection of expression helpers for the optimizer and execution engine.
 */
class ExpressionUtil {
 public:
  ExpressionUtil() = delete;

  /**
   * Populate the given set with all of the column oids referenced
   * in this expression tree.
   * @param col_oids place to store column oids in the expression
   * @param expr to extract column oids from
   */
  static void GetColumnOids(std::set<catalog::col_oid_t> *col_oids, common::ManagedPointer<AbstractExpression> expr) {
    // Recurse into our children
    for (const auto &child : expr->GetChildren()) {
      ExpressionUtil::GetColumnOids(col_oids, child);
    }

    // If our mofo is a ColumnValueExpression, then pull out our column ids
    auto etype = expr->GetExpressionType();
    if (etype == ExpressionType::COLUMN_VALUE) {
      auto t_expr = expr.CastManagedPointerTo<ColumnValueExpression>();
      col_oids->insert(t_expr->GetColumnOid());
    }
  }

  /**
   * Walks an expression trees and find all AggregationExprs subtrees.
   * @param aggr_exprs List of generated aggregate expressions
   * @param expr The abstract expression tree to be traversed
   */
  static void GetAggregateExprs(std::vector<common::ManagedPointer<AggregateExpression>> *aggr_exprs,
                                common::ManagedPointer<AbstractExpression> expr) {
    if (ExpressionUtil::IsAggregateExpression(expr->GetExpressionType())) {
      auto aggr_expr = expr.CastManagedPointerTo<AggregateExpression>();
      aggr_exprs->push_back(aggr_expr);
    } else {
      for (const auto &child : expr->GetChildren()) GetAggregateExprs(aggr_exprs, child);
    }
  }

  /**
   * Checks whether the AbstractExpression represents an aggregation
   * @param expr expression to check
   * @returns whether expr is an aggregate
   */
  static bool IsAggregateExpression(common::ManagedPointer<AbstractExpression> expr) {
    return IsAggregateExpression(expr->GetExpressionType());
  }

  /**
   * Checks whether the ExpressionType represents an aggregation
   * @param type ExpressionType to check
   * @returns whether type is an aggregate
   */
  static bool IsAggregateExpression(ExpressionType type) {
    switch (type) {
      case ExpressionType::AGGREGATE_COUNT:
      case ExpressionType::AGGREGATE_SUM:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_MAX:
      case ExpressionType::AGGREGATE_AVG:
      case ExpressionType::AGGREGATE_TOP_K:
      case ExpressionType::AGGREGATE_HISTOGRAM:
        return true;
      default:
        return false;
    }
  }

  /**
   * Checks whether the ExpressionType represents an operation
   * @param type ExpressionType to check
   * @returns whether type is an operation
   */
  static bool IsOperatorExpression(ExpressionType type) {
    switch (type) {
      case ExpressionType::OPERATOR_PLUS:
      case ExpressionType::OPERATOR_MINUS:
      case ExpressionType::OPERATOR_MULTIPLY:
      case ExpressionType::OPERATOR_DIVIDE:
      case ExpressionType::OPERATOR_CONCAT:
      case ExpressionType::OPERATOR_MOD:
      case ExpressionType::OPERATOR_CAST:
      case ExpressionType::OPERATOR_NOT:
      case ExpressionType::OPERATOR_IS_NULL:
      case ExpressionType::OPERATOR_EXISTS:
      case ExpressionType::OPERATOR_UNARY_MINUS:
        return true;
      default:
        return false;
    }
  }

  /**
   * @return True if the given expression type is a comparison expression; false otherwise.
   */
  static bool IsComparisonExpression(ExpressionType type) {
    switch (type) {
      case ExpressionType::COMPARE_EQUAL:
      case ExpressionType::COMPARE_NOT_EQUAL:
      case ExpressionType::COMPARE_LESS_THAN:
      case ExpressionType::COMPARE_GREATER_THAN:
      case ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
      case ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
      case ExpressionType::COMPARE_LIKE:
      case ExpressionType::COMPARE_NOT_LIKE:
      case ExpressionType::COMPARE_IN:
      case ExpressionType::COMPARE_IS_DISTINCT_FROM:
        return true;
      default:
        return false;
    }
  }

  /**
   * @return True if the given expression type is an arithmetic expression; false otherwise.
   */
  static bool IsArithmeticExpression(ExpressionType type) {
    switch (type) {
      case ExpressionType::OPERATOR_PLUS:
      case ExpressionType::OPERATOR_MINUS:
      case ExpressionType::OPERATOR_MULTIPLY:
      case ExpressionType::OPERATOR_DIVIDE:
      case ExpressionType::OPERATOR_CONCAT:
      case ExpressionType::OPERATOR_MOD:
        return true;
      default:
        return false;
    }
  }

  /**
   * @return True if the given expression type is a column-reference expression; false otherwise.
   */
  static bool IsColumnRefExpression(ExpressionType type) { return type == ExpressionType::COLUMN_VALUE; }

  /**
   * @return True if the given expression type is a constant value expression; false otherwise.
   */
  static bool IsConstantExpression(ExpressionType type) { return type == ExpressionType::VALUE_CONSTANT; }

  /**
   * @return True if the given expression type is a parameter value expression; false otherwise.
   */
  static bool IsParamExpression(ExpressionType type) { return type == ExpressionType::VALUE_PARAMETER; }

  /**
   * For a given comparison operator, reverses the comparison.
   * This function flips ExpressionType such that flipping the left and right
   * child of the original expression would still be logically equivalent.
   *
   * @param type ExpressionType (should be comparison type) to reverse
   * @returns the ExpressionType that is the logical reverse of the input
   */
  static ExpressionType ReverseComparisonExpressionType(ExpressionType type) {
    switch (type) {
      case ExpressionType::COMPARE_GREATER_THAN:
        return ExpressionType::COMPARE_LESS_THAN;
      case ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
        return ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO;
      case ExpressionType::COMPARE_LESS_THAN:
        return ExpressionType::COMPARE_GREATER_THAN;
      case ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
        return ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO;
      default:
        return type;
    }
  }

 public:
  /**
   * @brief TODO(boweic): this function may not be efficient, in the future we
   * may want to add expressions to groups so that we do not need to walk
   * through the expression tree when judging '==' each time
   *
   * Convert all expression in the current expression tree that is in
   * child_expr_map to tuple value expression with corresponding column offset
   * of the input child tuple. This is used for handling projection
   * on situations like aggregate function (e.g. SELECT sum(a)+max(b) FROM ...
   * GROUP BY ...) when input columns contain sum(a) and sum(b). We need to
   * treat them as tuple value expression in the projection plan. This function
   * should always be called before calling EvaluateExpression
   *
   * Please notice that this function should only apply to copied expression
   * since it would modify the current expression. We do not want to modify the
   * original expression since it may be referenced in other places
   *
   * @param expr The expression
   * @param child_expr_maps map from child column ids to expression
   * @returns Modified expression (expr is not modified)
   *
   * @note the value returned must be freed by the caller
   */
  static std::unique_ptr<AbstractExpression> ConvertExprCVNodes(
      common::ManagedPointer<AbstractExpression> expr, const std::vector<optimizer::ExprMap> &child_expr_maps) {
    if (expr.Get() == nullptr) {
      return nullptr;
    }

    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (size_t i = 0; i < expr->GetChildrenSize(); i++) {
      auto child_expr = expr->GetChild(i);

      bool did_insert = false;
      int tuple_idx = 0;
      for (auto &child_expr_map : child_expr_maps) {
        if (child_expr->GetExpressionType() != ExpressionType::COLUMN_VALUE && child_expr_map.count(child_expr) != 0U) {
          auto type = child_expr->GetReturnValueType();
          auto iter = child_expr_map.find(child_expr);
          NOISEPAGE_ASSERT(iter != child_expr_map.end(), "Missing ColumnValueExpression...");

          // Add to children directly because DerivedValueExpression has no children
          auto value_idx = static_cast<int>(iter->second);
          children.push_back(std::make_unique<DerivedValueExpression>(type, tuple_idx, value_idx));

          did_insert = true;
          break;
        }

        tuple_idx++;
      }

      if (!did_insert) {
        children.push_back(ConvertExprCVNodes(child_expr, child_expr_maps));
      }
    }

    // Return a copy with new children
    NOISEPAGE_ASSERT(children.size() == expr->GetChildrenSize(), "size not equal after walk");
    return expr->CopyWithChildren(std::move(children));
  }

  /**
   * Walks an expression tree and finds all AggregateExpression and ColumnValueExpressions
   * @param expr_set optimizer::ExprSet to store found expressions in
   * @param expr AbstractExpression to walk
   */
  static void GetTupleAndAggregateExprs(optimizer::ExprSet *expr_set, common::ManagedPointer<AbstractExpression> expr) {
    std::vector<common::ManagedPointer<AbstractExpression>> tv_exprs;
    std::vector<common::ManagedPointer<AbstractExpression>> aggr_exprs;
    GetTupleAndAggregateExprs(&aggr_exprs, &tv_exprs, expr);
    for (auto &tv_expr : tv_exprs) {
      expr_set->insert(tv_expr);
    }
    for (auto &aggr_expr : aggr_exprs) {
      expr_set->insert(aggr_expr);
    }
  }

  /**
   * Walks an expression tree and finds all AggregateExpression and ColumnValueExpressions.
   * expr_map keeps track of the order they were inserted in. ColumnValueExpressions are
   * added before AggregateExpressions to expr_map.
   *
   * @param expr_map optimizer::ExprMap to store found expressions and order they were added
   * @param expr AbstractExpression to walk
   */
  static void GetTupleAndAggregateExprs(optimizer::ExprMap *expr_map, common::ManagedPointer<AbstractExpression> expr) {
    std::vector<common::ManagedPointer<AbstractExpression>> tv_exprs;
    std::vector<common::ManagedPointer<AbstractExpression>> aggr_exprs;
    GetTupleAndAggregateExprs(&aggr_exprs, &tv_exprs, expr);
    for (auto &tv_expr : tv_exprs) {
      if (expr_map->count(tv_expr) == 0U) {
        expr_map->emplace(tv_expr, expr_map->size());
      }
    }
    for (auto &aggr_expr : aggr_exprs) {
      if (expr_map->count(aggr_expr) == 0U) {
        expr_map->emplace(aggr_expr, expr_map->size());
      }
    }
  }

  /**
   * Walks an expression trees and find all AggregateExpression subtrees.
   * When this function returns, aggr_exprs contains all AggregateExpression in the expression
   * specified by expr in the order they were found in.
   *
   * @param aggr_exprs vector to store found AggregateExpressions
   * @param expr Expression to walk
   */
  static void GetAggregateExprs(std::vector<common::ManagedPointer<AbstractExpression>> *aggr_exprs,
                                common::ManagedPointer<AbstractExpression> expr) {
    std::vector<common::ManagedPointer<AbstractExpression>> dummy_tv_exprs;
    GetTupleAndAggregateExprs(aggr_exprs, &dummy_tv_exprs, expr);
  }

  /**
   * Walks an expression trees and find all AggregateExpression and ColumnValueExpressions.
   * AggregateExpression and ColumnValueExpression are appended to the respective vectors in
   * the order that each are found in.
   *
   * @param aggr_exprs vector of AggregateExpressions in expression
   * @param tv_exprs vector of ColumnValueExpressions in expression
   * @param expr Expression to walk
   */
  static void GetTupleAndAggregateExprs(std::vector<common::ManagedPointer<AbstractExpression>> *aggr_exprs,
                                        std::vector<common::ManagedPointer<AbstractExpression>> *tv_exprs,
                                        common::ManagedPointer<AbstractExpression> expr) {
    size_t children_size = expr->GetChildrenSize();
    if (IsAggregateExpression(expr->GetExpressionType())) {
      aggr_exprs->push_back(expr);
    } else if (expr->GetExpressionType() == ExpressionType::COLUMN_VALUE) {
      tv_exprs->push_back(expr);
    } else {
      NOISEPAGE_ASSERT(expr->GetExpressionType() != ExpressionType::VALUE_TUPLE,
                       "DerivedValueExpression should not exist here.");
      for (size_t i = 0; i < children_size; i++) {
        GetTupleAndAggregateExprs(aggr_exprs, tv_exprs, expr->GetChild(i));
      }
    }
  }

  /**
   * Walks an expression trees and find all ColumnValueExpressions in the tree.
   * ColumnValueExpressions are added to a expr_map to preserve order they are found in.
   * The expr_map is updated in post-order traversal order.
   *
   * @param expr_map map to place found ColumnValueExpressions for order-preserving
   * @param expr Expression to walk
   */
  static void GetTupleValueExprs(optimizer::ExprMap *expr_map, common::ManagedPointer<AbstractExpression> expr) {
    size_t children_size = expr->GetChildrenSize();
    for (size_t i = 0; i < children_size; i++) {
      GetTupleValueExprs(expr_map, expr->GetChild(i));
    }

    // Here we need a deep copy to void double delete subtree
    if (expr->GetExpressionType() == ExpressionType::COLUMN_VALUE) {
      expr_map->emplace(expr, expr_map->size());
    }
  }

  /**
   * Walks an expression trees and find all ColumnValueExpressions in the tree
   * ColumnValueExpressions are added to the expr_set in post-order traversal.
   *
   * @param expr_set set to place found ColumnValueExpressions
   * @param expr Expression to walk
   */
  static void GetTupleValueExprs(optimizer::ExprSet *expr_set, common::ManagedPointer<AbstractExpression> expr) {
    size_t children_size = expr->GetChildrenSize();
    for (size_t i = 0; i < children_size; i++) {
      GetTupleValueExprs(expr_set, expr->GetChild(i));
    }

    if (expr->GetExpressionType() == ExpressionType::COLUMN_VALUE) {
      expr_set->insert(expr);
    }
  }

  /**
   * Walks an expression trees. Set the value_idx for the leaf tuple_value_expr
   * Deduce the return value type of expression. Set the function ptr for
   * function expression.
   *
   * Plz notice: this function should only be used in the optimizer.
   *
   * @param expr_maps expr_maps containing relevant mappings to set value_idx
   * @param expr Expression to walk
   * @returns Evaluated AbstractExpression
   */
  static std::unique_ptr<AbstractExpression> EvaluateExpression(const std::vector<optimizer::ExprMap> &expr_maps,
                                                                common::ManagedPointer<AbstractExpression> expr) {
    // To evaluate the return type, we need a bottom up approach.
    if (expr.Get() == nullptr) {
      return nullptr;
    }

    // Evaluate all children and store new children pointers
    size_t children_size = expr->GetChildrenSize();
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (size_t i = 0; i < children_size; i++) {
      children.push_back(EvaluateExpression(expr_maps, expr->GetChild(i)));
    }

    if (expr->GetExpressionType() == ExpressionType::COLUMN_VALUE) {
      // Point to the correct column returned in the logical tuple underneath
      auto c_tup_expr = expr.CastManagedPointerTo<ColumnValueExpression>();
      NOISEPAGE_ASSERT(children_size == 0, "ColumnValueExpression should have 0 children");

      int tuple_idx = 0;
      for (auto &expr_map : expr_maps) {
        auto iter = expr_map.find(expr);
        if (iter != expr_map.end()) {
          // Create DerivedValueExpression (iter->second is value_idx)
          auto type = c_tup_expr->GetReturnValueType();
          return std::make_unique<DerivedValueExpression>(type, tuple_idx, iter->second);
        }
        ++tuple_idx;
      }

      // Technically, EvaluateExpression should replace all ColumnValueExpressions with
      // DerivedValueExpressions using expr_maps in order for execution to make sense,
      // otherwise we have ColumnValues that don't point into previous tuples....
      //
      // However, there are other cases where EvaluateExpression will result in an unbound
      // ColumnValueExpression particularly when dealing with InnerIndexJoin.
      OPTIMIZER_LOG_TRACE("EvaluateExpression resulted in an unbound ColumnValueExpression");
    } else if (IsAggregateExpression(expr->GetExpressionType())) {
      // if aggregate expression exists in the children expression map
      // make a derived value expression to avoid double computation
      auto c_aggr_expr = expr.CastManagedPointerTo<AggregateExpression>();
      NOISEPAGE_ASSERT(c_aggr_expr, "expr should be AggregateExpression");

      int tuple_idx = 0;
      for (auto &expr_map : expr_maps) {
        auto iter = expr_map.find(expr);
        if (iter != expr_map.end()) {
          // Create DerivedValueExpression (iter->second is value_idx)
          auto type = c_aggr_expr->GetReturnValueType();
          return std::make_unique<DerivedValueExpression>(type, tuple_idx, iter->second);
        }
        ++tuple_idx;
      }

    } else if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
      /*
      TODO(wz2): Uncomment and fix this when Functions exist
      auto func_expr = (expression::FunctionExpression *)expr;
      std::vector<type::TypeId> argtypes;
      for (size_t i = 0; i < children_size; i++)
        argtypes.push_back(expr->GetChild(i)->GetValueType());
      // Check and set the function ptr
      auto catalog = catalog::Catalog::GetInstance();
      const catalog::FunctionData &func_data =
          catalog->GetFunction(func_expr->GetFuncName(), argtypes);
      LOG_DEBUG("Function %s found in the catalog",
                func_data.func_name_.c_str());
      LOG_DEBUG("Argument num: %ld", func_data.argument_types_.size());
      if (!func_data.is_udf_) {
        func_expr->SetBuiltinFunctionExpressionParameters(
            func_data.func_, func_data.return_type_, func_data.argument_types_);
      } else {
        func_expr->SetUDFFunctionExpressionParameters(
            func_data.func_context_, func_data.return_type_,
            func_data.argument_types_);
      }
      */
    } else if (expr->GetExpressionType() == ExpressionType::OPERATOR_CASE_EXPR) {
      auto case_expr = expr.CastManagedPointerTo<CaseExpression>();
      NOISEPAGE_ASSERT(children_size == 0, "CaseExpression should have 0 children");

      // Evaluate against WhenClause condition + result and store new
      std::vector<CaseExpression::WhenClause> clauses;
      for (size_t i = 0; i < case_expr->GetWhenClauseSize(); i++) {
        auto cond = EvaluateExpression(expr_maps, case_expr->GetWhenClauseCondition(i));
        auto result = EvaluateExpression(expr_maps, case_expr->GetWhenClauseResult(i));
        clauses.emplace_back(CaseExpression::WhenClause{std::move(cond), std::move(result)});
      }

      // Create and return new CaseExpression that is evaluated
      auto def_cond = EvaluateExpression(expr_maps, case_expr->GetDefaultClause());
      auto type = case_expr->GetReturnValueType();
      return std::make_unique<CaseExpression>(type, std::move(clauses), std::move(def_cond));
    } else if (expr->GetExpressionType() == ExpressionType::OPERATOR_CAST) {
      NOISEPAGE_ASSERT(children_size == 1, "TypeCastExpression should have exactly 1 child.");
      return expr->GetChild(0)->Copy();
    }

    return expr->CopyWithChildren(std::move(children));
  }

  /**
   * Check whether two vectors of expression equal to each other.
   *
   * @param l vector of one set of expressions
   * @param r vector of other set of expressions
   * @param ordered Whether comparison should consider the order
   * @returns Whether two vectors of expressions are equal or not
   */
  static bool EqualExpressions(const std::vector<common::ManagedPointer<AbstractExpression>> &l,
                               const std::vector<common::ManagedPointer<AbstractExpression>> &r, bool ordered = false) {
    if (l.size() != r.size()) return false;
    // Consider expression order in the comparison
    if (ordered) {
      size_t num_exprs = l.size();
      for (size_t i = 0; i < num_exprs; i++) {
        if (*l[i] == *r[i]) return false;
      }

      return true;
    }

    optimizer::ExprSet l_set, r_set;
    for (auto expr : l) l_set.insert(expr);
    for (auto expr : r) r_set.insert(expr);
    return l_set == r_set;
  }

  /**
   * Joins all AnnotatedExpression in vector by AND operators.
   * The input AnnotatedExpression are copied before being joined together.
   *
   * @param exprs Input vector of AnnotatedExpressions to join together
   * @returns Joined together AbstractExpression using AND operators.
   *
   * @note returned expression needs to be freed by the caller
   */
  static std::unique_ptr<AbstractExpression> JoinAnnotatedExprs(
      const std::vector<optimizer::AnnotatedExpression> &exprs) {
    if (exprs.empty()) {
      return nullptr;
    }

    std::vector<std::unique_ptr<AbstractExpression>> children;
    children.push_back(exprs[0].GetExpr()->Copy());
    for (size_t i = 1; i < exprs.size(); ++i) {
      children.push_back(exprs[i].GetExpr()->Copy());

      auto shared = std::make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(children));

      // Silence Clang!
      std::vector<std::unique_ptr<AbstractExpression>> new_children;
      new_children.push_back(std::move(shared));
      children = std::move(new_children);
    }

    NOISEPAGE_ASSERT(children.size() == 1, "children should have exactly 1 AbstractExpression");
    return std::move(children[0]);
  }

  /**
   * @return True if the given expression is of the form: col op const_val. False otherwise.
   */
  static bool IsColumnCompareWithConst(const parser::AbstractExpression &expr) {
    if (expr.GetChildrenSize() != 2) {
      return false;
    }
    return IsComparisonExpression(expr.GetExpressionType()) &&
           IsColumnRefExpression(expr.GetChild(0)->GetExpressionType()) &&
           IsConstantExpression(expr.GetChild(1)->GetExpressionType());
  }

  /**
   * @return True if the given expression is of the form: col op param_val. False otherwise.
   */
  static bool IsColumnCompareWithParam(const parser::AbstractExpression &expr) {
    if (expr.GetChildrenSize() != 2) {
      return false;
    }
    return IsComparisonExpression(expr.GetExpressionType()) &&
           IsColumnRefExpression(expr.GetChild(0)->GetExpressionType()) &&
           IsParamExpression(expr.GetChild(1)->GetExpressionType());
  }

  /**
   * @return True if the given expression is of the form: const op col. False otherwise.
   */
  static bool IsConstCompareWithColumn(const parser::AbstractExpression &expr) {
    if (expr.GetChildrenSize() != 2) {
      return false;
    }
    return IsComparisonExpression(expr.GetExpressionType()) &&
           IsConstantExpression(expr.GetChild(0)->GetExpressionType()) &&
           IsColumnRefExpression(expr.GetChild(1)->GetExpressionType());
  }
};

}  // namespace noisepage::parser
