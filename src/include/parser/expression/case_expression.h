#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "parser/expression/abstract_expression.h"

namespace noisepage::parser {
/**
 * CaseExpression represents a SQL WHEN ... THEN ... statement.
 */
class CaseExpression : public AbstractExpression {
 public:
  /** WHEN ... THEN ... clauses. */
  struct WhenClause {
    /** The condition to be checked for this case expression. */
    std::unique_ptr<AbstractExpression> condition_;
    /** The value that this expression should have if the corresponding condition is true. */
    std::unique_ptr<AbstractExpression> then_;

    /**
     * Equality check
     * @param rhs the other WhenClause to compare to
     * @return if the two are equal
     */
    bool operator==(const WhenClause &rhs) const { return *condition_ == *rhs.condition_ && *then_ == *rhs.then_; }

    /**
     * Inequality check
     * @param rhs the other WhenClause to compare toz
     * @return if the two are not equal
     */
    bool operator!=(const WhenClause &rhs) const { return !operator==(rhs); }

    /**
     * Hash the current WhenClause.
     * @return hash of WhenClause
     */
    common::hash_t Hash() const;

    /**
     * Derived expressions should call this base method
     * @return expression serialized to json
     */
    nlohmann::json ToJson() const;

    /**
     * Derived expressions should call this base method
     * @param j json to deserialize
     */
    std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j);
  };

  /**
   * Instantiate a new case expression.
   * @param return_value_type return value of the case expression
   * @param when_clauses list of WhenClauses
   * @param default_expr default expression for this case
   */
  CaseExpression(const type::TypeId return_value_type, std::vector<WhenClause> &&when_clauses,
                 std::unique_ptr<AbstractExpression> default_expr)
      : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR, return_value_type, {}),
        when_clauses_(std::move(when_clauses)),
        default_expr_(std::move(default_expr)) {}

  /** Default constructor for deserialization. */
  CaseExpression() = default;

  /**
   * Hashe the current case expression.
   * @return hash of CaseExpression
   */
  common::hash_t Hash() const override;

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two expressions are logically equal
   */
  bool operator==(const AbstractExpression &rhs) const override;

  /**
   * Copies this CaseExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    NOISEPAGE_ASSERT(children.empty(), "CaseExpression should have no children");
    return Copy();
  }

  /**
   * @return the number of WhenClauses
   */
  size_t GetWhenClauseSize() const { return when_clauses_.size(); }

  /**
   * @param index index of WhenClause to get
   * @return condition at that index
   */
  common::ManagedPointer<AbstractExpression> GetWhenClauseCondition(size_t index) const {
    NOISEPAGE_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return common::ManagedPointer(when_clauses_[index].condition_);
  }

  /**
   * @param index index of WhenClause to get
   * @return result at that index
   */
  common::ManagedPointer<AbstractExpression> GetWhenClauseResult(size_t index) const {
    NOISEPAGE_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return common::ManagedPointer(when_clauses_[index].then_);
  }

  /** @return default clause, if it exists */
  common::ManagedPointer<AbstractExpression> GetDefaultClause() const { return common::ManagedPointer(default_expr_); }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override;

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override;

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /** List of condition and result cases: WHEN ... THEN ... */
  std::vector<WhenClause> when_clauses_;
  /** Default result case. */
  std::unique_ptr<AbstractExpression> default_expr_;
};

DEFINE_JSON_HEADER_DECLARATIONS(CaseExpression::WhenClause);
DEFINE_JSON_HEADER_DECLARATIONS(CaseExpression);

}  // namespace noisepage::parser
