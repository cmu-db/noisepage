#include <vector>
#include <unordered_set>

#include "optimizer/optimizer_defs.h"
#include "optimizer/util.h"
#include "parser/expression_util.h"

namespace terrier::optimizer::util {

void ExtractEquiJoinKeys(
    const std::vector<AnnotatedExpression> join_predicates,
    std::vector<common::ManagedPointer<parser::AbstractExpression>> *left_keys,
    std::vector<common::ManagedPointer<parser::AbstractExpression>> *right_keys,
    const std::unordered_set<std::string> &left_alias,
    const std::unordered_set<std::string> &right_alias) {

  for (auto &expr_unit : join_predicates) {
    auto expr = expr_unit.GetExpr();
    if (expr->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL) {
      auto l_expr = expr->GetChild(0);
      auto r_expr = expr->GetChild(1);

      // equi-join between two TupleValueExpressions
      if (l_expr->GetExpressionType() == parser::ExpressionType::VALUE_TUPLE &&
          r_expr->GetExpressionType() == parser::ExpressionType::VALUE_TUPLE) {
        auto l_tv_expr = l_expr.CastManagedPointerTo<const parser::TupleValueExpression>();
        auto r_tv_expr = r_expr.CastManagedPointerTo<const parser::TupleValueExpression>();
        auto l_expr = const_cast<parser::TupleValueExpression*>(l_tv_expr.get());
        auto r_expr = const_cast<parser::TupleValueExpression*>(r_tv_expr.get());

        // Assign keys based on left and right join tables
        // l_tv_expr/r_tv_expr should not be modified later...
        if (left_alias.find(l_tv_expr->GetTableName()) != left_alias.end() &&
            right_alias.find(r_tv_expr->GetTableName()) != right_alias.end()) {
          left_keys->emplace_back(l_expr);
          right_keys->emplace_back(r_expr);
        } else if (left_alias.find(r_tv_expr->GetTableName()) != left_alias.end() &&
                   right_alias.find(l_tv_expr->GetTableName()) != right_alias.end()) {
          left_keys->emplace_back(r_expr);
          right_keys->emplace_back(l_expr);
        }
      }
    }
  }
}

}  // namespace terrier::optimizer::util
