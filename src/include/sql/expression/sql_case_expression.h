#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "common/macros.h"
#include "parser/expression_defs.h"
#include "sql/expression/sql_abstract_expression.h"

namespace terrier::sql {

/**
 * Represents a logical case expression.
 */
class SqlCaseExpression : public SqlAbstractExpression {
 public:
  /**
   * WHEN ... THEN ... clauses.
   */
  struct WhenClause {
    /**
     * The condition to be checked for this case expression.
     */
    std::shared_ptr<SqlAbstractExpression> condition;
    /**
     * The value that this expression should have if the corresponding condition is true.
     */
    std::shared_ptr<SqlAbstractExpression> then;

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

  common::hash_t Hash() const override {
    common::hash_t hash = SqlAbstractExpression::Hash();
    for (auto &clause : when_clauses_) {
      hash = common::HashUtil::CombineHashes(hash, clause.condition->Hash());
      hash = common::HashUtil::CombineHashes(hash, clause.then->Hash());
    }
    if (default_expr_ != nullptr) {
      hash = common::HashUtil::CombineHashes(hash, default_expr_->Hash());
    }
    return hash;
  }

  bool operator==(const SqlAbstractExpression &rhs) const override {
    if (GetExpressionType() != rhs.GetExpressionType()) return false;
    auto const &other = dynamic_cast<const SqlCaseExpression &>(rhs);
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

  std::unique_ptr<SqlAbstractExpression> Copy() const override { return std::make_unique<SqlCaseExpression>(*this); }

  /**
   * @return the number of when clauses
   */
  size_t GetWhenClauseSize() const { return when_clauses_.size(); }

  /**
   * @param index index of when clause to get
   * @return condition at that index
   */
  std::shared_ptr<SqlAbstractExpression> GetWhenClauseCondition(size_t index) const {
    TERRIER_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return when_clauses_[index].condition;
  }

  /**
   * @param index index of when clause to get
   * @return result at that index
   */
  std::shared_ptr<SqlAbstractExpression> GetWhenClauseResult(size_t index) const {
    TERRIER_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return when_clauses_[index].then;
  }

  /**
   * @return default clause, if it exists
   */
  std::shared_ptr<SqlAbstractExpression> GetDefaultClause() const { return default_expr_; }

  /**
   * Builder for building a SqlCaseExpression
   */
  class Builder : public SqlAbstractExpression::Builder<Builder> {
   public:
    Builder &SetWhenClauses(std::vector<WhenClause> when_clauses) {
      when_clauses_ = std::move(when_clauses);
      return *this;
    }

    Builder &SetDefaultExpr(std::shared_ptr<SqlAbstractExpression> default_expr) {
      default_expr_ = std::move(default_expr);
      return *this;
    }

    std::shared_ptr<SqlCaseExpression> Build() {
      return std::shared_ptr<SqlCaseExpression>(
          new SqlCaseExpression(return_value_type_, std::move(when_clauses_), default_expr_));
    }

   private:
    std::vector<WhenClause> when_clauses_;
    std::shared_ptr<SqlAbstractExpression> default_expr_;
  };
  friend class Builder;

 private:
  const std::vector<WhenClause> when_clauses_;
  const std::shared_ptr<SqlAbstractExpression> default_expr_;

  /**
   * Instantiate a new case expression.
   * @param return_value_type return value of the case expression
   * @param when_clauses list of when clauses
   * @param default_expr default expression for this case
   */
  SqlCaseExpression(const type::TypeId return_value_type, std::vector<WhenClause> &&when_clauses,
                    std::shared_ptr<SqlAbstractExpression> default_expr)
      : SqlAbstractExpression(parser::ExpressionType::OPERATOR_CASE_EXPR, return_value_type, {}),
        when_clauses_(std::move(when_clauses)),
        default_expr_(std::move(default_expr)) {}
};

}  // namespace terrier::sql
