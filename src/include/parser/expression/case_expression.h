#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "common/macros.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "parser/sql_node_visitor.h"

namespace terrier::parser {

/**
 * Represents a logical case expression.
 */
class CaseExpression : public AbstractExpression {
 public:
  /**
   * WHEN ... THEN ... clauses.
   */
  struct WhenClause {
    /**
     * The condition to be checked for this case expression.
     */
    std::shared_ptr<AbstractExpression> condition;
    /**
     * The value that this expression should have if the corresponding condition is true.
     */
    std::shared_ptr<AbstractExpression> then;

    /**
     * Equality check
     * @param rhs the other WhenClause to compare to
     * @return if the two are equal
     */
    bool operator==(const WhenClause &rhs) const { return *condition == *rhs.condition && *then == *rhs.then; }
    /**
     * Inequality check
     * @param rhs the other WhenClause to compare toz
     * @return if the two are not equal
     */
    bool operator!=(const WhenClause &rhs) const { return !operator==(rhs); }
  };

  /**
   * Instantiate a new case expression.
   * @param return_value_type return value of the case expression
   * @param when_clauses list of when clauses
   * @param default_expr default expression for this case
   */
  CaseExpression(const type::TypeId return_value_type, std::vector<WhenClause> &&when_clauses,
                 std::shared_ptr<AbstractExpression> default_expr)
      : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR, return_value_type, {}),
        when_clauses_(std::move(when_clauses)),
        default_expr_(std::move(default_expr)) {}

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    for (auto &clause : when_clauses_) {
      hash = common::HashUtil::CombineHashes(hash, clause.condition->Hash());
      hash = common::HashUtil::CombineHashes(hash, clause.then->Hash());
    }
    if (default_expr_ != nullptr) {
      hash = common::HashUtil::CombineHashes(hash, default_expr_->Hash());
    }
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (GetExpressionType() != rhs.GetExpressionType()) return false;
    auto const &other = dynamic_cast<const CaseExpression &>(rhs);
    auto clause_size = GetWhenClauseSize();
    if (clause_size != other.GetWhenClauseSize()) return false;

    for (size_t i = 0; i < clause_size; i++)
      if (when_clauses_[i] != other.when_clauses_[i]) return false;

    auto default_exp = GetDefaultClause();
    auto other_default_exp = other.GetDefaultClause();
    if (default_exp == nullptr && other_default_exp == nullptr) return true;
    if (default_exp == nullptr || other_default_exp == nullptr) return false;
    return (*default_exp == *other_default_exp);
  }

  std::unique_ptr<AbstractExpression> Copy() const override { return std::make_unique<CaseExpression>(*this); }

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return the number of when clauses
   */
  size_t GetWhenClauseSize() const { return when_clauses_.size(); }

  /**
   * @param index index of when clause to get
   * @return condition at that index
   */
  std::shared_ptr<AbstractExpression> GetWhenClauseCondition(size_t index) const {
    TERRIER_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return when_clauses_[index].condition;
  }

  /**
   * @param index index of when clause to get
   * @return result at that index
   */
  std::shared_ptr<AbstractExpression> GetWhenClauseResult(size_t index) const {
    TERRIER_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return when_clauses_[index].then;
  }

  /**
   * @return default clause, if it exists
   */
  std::shared_ptr<AbstractExpression> GetDefaultClause() const { return default_expr_; }

 private:
  std::vector<WhenClause> when_clauses_;
  std::shared_ptr<AbstractExpression> default_expr_;
};

}  // namespace terrier::parser
