#include "optimizer/optimizer_defs.h"
#include "optimizer/util.h"
#include "parser/expression_util.h"

namespace terrier {
namespace optimizer {
namespace util {

std::vector<AnnotatedExpression> ExtractPredicates(
    std::shared_ptr<parser::AbstractExpression> expr,
    std::vector<AnnotatedExpression> annotated_predicates) {

  // Split a complex predicate into a set of predicates connected by AND.
  std::vector<std::shared_ptr<parser::AbstractExpression>> predicates;
  SplitPredicates(expr, predicates);

  for (auto predicate : predicates) {
    std::unordered_set<std::string> table_alias_set;
    parser::ExpressionUtil::GenerateTableAliasSet(predicate, table_alias_set);

    // Deep copy expression to avoid memory leak
    // (wz2) We shouldn't have to deep copy due to shared_ptr
    annotated_predicates.emplace_back(AnnotatedExpression(predicate, table_alias_set));
  }

  return annotated_predicates;
}

void SplitPredicates(std::shared_ptr<parser::AbstractExpression> expr,
                    std::vector<std::shared_ptr<parser::AbstractExpression>> &predicates) {

  if (expr->GetExpressionType() == parser::ExpressionType::CONJUNCTION_AND) {
    // Traverse down the expression tree along conjunction
    for (size_t i = 0; i < expr->GetChildrenSize(); i++) {
      SplitPredicates(expr->GetChild(i), predicates);
    }
  } else {
    // Find an expression that is the child of conjunction expression
    predicates.push_back(expr);
  }
}

std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>
ConstructSelectElementMap(std::vector<std::shared_ptr<parser::AbstractExpression>> &select_list) {

  std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>> res;
  for (auto &expr : select_list) {
    std::string alias;
    if (!expr->GetAlias().empty()) {
      // Expression has an alias
      alias = expr->GetAlias();
    } else if (expr->GetExpressionType() == parser::ExpressionType::VALUE_TUPLE) {
      // TupleValueExpression always has alias (column name)
      auto tv_expr = std::dynamic_pointer_cast<parser::TupleValueExpression>(expr);
      TERRIER_ASSERT(tv_expr, "expr should be TupleValueExpression");

      alias = tv_expr->GetColumnName();
    } else {
      continue;
    }

    // Store into mapping
    std::transform(alias.begin(), alias.end(), alias.begin(), ::tolower);
    res[alias] = expr;
  }

  return res;
};

void ExtractEquiJoinKeys(
    const std::vector<AnnotatedExpression> join_predicates,
    std::vector<std::shared_ptr<parser::AbstractExpression>> &left_keys,
    std::vector<std::shared_ptr<parser::AbstractExpression>> &right_keys,
    const std::unordered_set<std::string> &left_alias,
    const std::unordered_set<std::string> &right_alias) {

  for (auto &expr_unit : join_predicates) {
    if (expr_unit.expr->GetExpressionType() == parser::ExpressionType::COMPARE_EQUAL) {
      auto l_expr = expr_unit.expr->GetChild(0);
      auto r_expr = expr_unit.expr->GetChild(1);

      // equi-join between two TupleValueExpressions
      if (l_expr->GetExpressionType() == parser::ExpressionType::VALUE_TUPLE &&
          r_expr->GetExpressionType() == parser::ExpressionType::VALUE_TUPLE) {
        auto l_tv_expr = std::dynamic_pointer_cast<parser::TupleValueExpression>(l_expr);
        auto r_tv_expr = std::dynamic_pointer_cast<parser::TupleValueExpression>(r_expr);
        TERRIER_ASSERT(l_tv_expr && r_tv_expr, "l_expr/r_expr should be TupleValueExpression");

        // Assign keys based on left and right join tables
        if (left_alias.find(l_tv_expr->GetTableName()) != left_alias.end() &&
            right_alias.find(r_tv_expr->GetTableName()) != right_alias.end()) {
          left_keys.emplace_back(l_tv_expr->Copy());
          right_keys.emplace_back(r_tv_expr->Copy());
        } else if (left_alias.find(r_tv_expr->GetTableName()) != left_alias.end() &&
                   right_alias.find(l_tv_expr->GetTableName()) != right_alias.end()) {
          left_keys.emplace_back(r_tv_expr->Copy());
          right_keys.emplace_back(l_tv_expr->Copy());
        }
      }
    }
  }
}

}  // namespace util
}  // namespace optimizer
}  // namespace terrier
