#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression/expression_defs.h"

namespace terrier {
namespace parser {
namespace expression {

/**
 * Represents a logical case expression.
 */
class CaseExpression : public AbstractExpression {
 public:
  /**
   * WHEN ... THEN ... clauses.
   */
  using WhenClause = struct {
    /**
     * The condition to be checked for this case expression.
     */
    std::unique_ptr<AbstractExpression> condition;
    /**
     * The value that this expression should have if the corresponding condition is true.
     */
    std::unique_ptr<AbstractExpression> then;
  };

  /**
   * Instantiate a new case expression.
   * @param return_value_type return value of the case expression
   * @param when_clauses list of when clauses
   * @param default_expr default expression for this case
   */
  CaseExpression(const type::TypeId return_value_type, std::vector<WhenClause> *when_clauses,
                 std::unique_ptr<AbstractExpression> default_expr)
      : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR, return_value_type,
                           std::vector<std::unique_ptr<AbstractExpression>>()),
        when_clauses_(std::move(*when_clauses)),
        default_expr_(std::move(default_expr)) {}

  hash_t Hash() const override {
    ExpressionType expr = GetExpressionType();
    hash_t hash = HashUtil::Hash(&expr);
    for (auto &clause : when_clauses_) {
      hash = HashUtil::CombineHashes(hash, clause.condition->Hash());
      hash = HashUtil::CombineHashes(hash, clause.then->Hash());
    }
    if (default_expr_ != nullptr) {
      hash = HashUtil::CombineHashes(hash, default_expr_->Hash());
    }
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (GetExpressionType() != rhs.GetExpressionType()) return false;
    auto const &other = dynamic_cast<const expression::CaseExpression &>(rhs);
    auto clause_size = GetWhenClauseSize();
    if (clause_size != other.GetWhenClauseSize()) return false;

    for (size_t i = 0; i < clause_size; i++) {
      if (*GetWhenClauseCondition(i) != *other.GetWhenClauseCondition(i)) return false;
      if (*GetWhenClauseResult(i) != *other.GetWhenClauseResult(i)) return false;
    }
    auto *default_exp = GetDefaultClause();
    auto *other_default_exp = other.GetDefaultClause();
    if (default_exp == nullptr && other_default_exp == nullptr) return true;
    if (default_exp == nullptr || other_default_exp == nullptr) return false;
    return (*default_exp == *other_default_exp);
  }

  bool operator!=(const AbstractExpression &rhs) const override { return !(*this == rhs); }

  AbstractExpression *Copy() const override {
    std::vector<WhenClause> copied_clauses;
    for (auto &clause : when_clauses_) {
      copied_clauses.push_back(WhenClause{std::unique_ptr<AbstractExpression>(clause.condition->Copy()),
                                          std::unique_ptr<AbstractExpression>(clause.then->Copy())});
    }
    return new CaseExpression(
        GetReturnValueType(), &copied_clauses,
        default_expr_ == nullptr ? nullptr : std::unique_ptr<AbstractExpression>(default_expr_->Copy()));
  }

  /**
   * Get the number of when clauses.
   * @return the number of when clauses
   */
  size_t GetWhenClauseSize() const { return when_clauses_.size(); }

  /**
   * Get the condition for a particular when clause.
   * @param index index of when clause to get
   * @return condition at that index
   */
  AbstractExpression *GetWhenClauseCondition(size_t index) const {
    if (index >= when_clauses_.size()) return nullptr;
    return when_clauses_[index].condition.get();
  }

  /**
   * Get the result for a particular when clause.
   * @param index index of when clause to get
   * @return result at that index
   */
  AbstractExpression *GetWhenClauseResult(size_t index) const {
    if (index >= when_clauses_.size()) return nullptr;
    return when_clauses_[index].then.get();
  }

  /**
   * Return the default clause of this case expression, if it exists.
   * @return default clause
   */
  AbstractExpression *GetDefaultClause() const { return default_expr_.get(); }

 private:
  std::vector<WhenClause> when_clauses_;
  std::unique_ptr<AbstractExpression> default_expr_;
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
