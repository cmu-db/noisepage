#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "common/macros.h"
#include "parser/expression/abstract_expression.h"

namespace terrier::parser {
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
    common::hash_t Hash() const {
      common::hash_t hash = condition_->Hash();
      hash = common::HashUtil::CombineHashes(hash, condition_->Hash());
      hash = common::HashUtil::CombineHashes(hash, then_->Hash());
      return hash;
    }

    /**
     * Derived expressions should call this base method
     * @return expression serialized to json
     */
    nlohmann::json ToJson() const {
      nlohmann::json j;
      j["condition"] = condition_->ToJson();
      j["then"] = then_->ToJson();
      return j;
    }

    /**
     * Derived expressions should call this base method
     * @param j json to deserialize
     */
    std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) {
      std::vector<std::unique_ptr<AbstractExpression>> exprs;
      auto deserialized_cond = DeserializeExpression(j.at("condition"));
      condition_ = std::move(deserialized_cond.result_);
      exprs.insert(exprs.end(), std::make_move_iterator(deserialized_cond.non_owned_exprs_.begin()),
                   std::make_move_iterator(deserialized_cond.non_owned_exprs_.end()));
      auto deserialized_then = DeserializeExpression(j.at("then"));
      then_ = std::move(deserialized_then.result_);
      exprs.insert(exprs.end(), std::make_move_iterator(deserialized_then.non_owned_exprs_.begin()),
                   std::make_move_iterator(deserialized_then.non_owned_exprs_.end()));
      return exprs;
    }
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

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    for (auto &clause : when_clauses_) {
      hash = common::HashUtil::CombineHashes(hash, clause.Hash());
    }
    if (default_expr_ != nullptr) {
      hash = common::HashUtil::CombineHashes(hash, default_expr_->Hash());
    }
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
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

  /**
   * Copies this CaseExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override {
    std::vector<WhenClause> clauses;
    for (const auto &clause : when_clauses_) {
      clauses.emplace_back(WhenClause{clause.condition_->Copy(), clause.then_->Copy()});
    }
    auto expr = std::make_unique<CaseExpression>(GetReturnValueType(), std::move(clauses), default_expr_->Copy());
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    TERRIER_ASSERT(children.empty(), "CaseExpression should have no children");
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
    TERRIER_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return common::ManagedPointer(when_clauses_[index].condition_);
  }

  /**
   * @param index index of WhenClause to get
   * @return result at that index
   */
  common::ManagedPointer<AbstractExpression> GetWhenClauseResult(size_t index) const {
    TERRIER_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return common::ManagedPointer(when_clauses_[index].then_);
  }

  /** @return default clause, if it exists */
  common::ManagedPointer<AbstractExpression> GetDefaultClause() const { return common::ManagedPointer(default_expr_); }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v,
              common::ManagedPointer<binder::BinderSherpa> sherpa) override {
    v->Visit(common::ManagedPointer(this), sherpa);
  }

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    std::vector<nlohmann::json> when_clauses_json;
    for (const auto &when_clause : when_clauses_) {
      when_clauses_json.push_back(when_clause.ToJson());
    }
    j["when_clauses"] = when_clauses_json;
    j["default_expr"] = default_expr_->ToJson();
    return j;
  }

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    when_clauses_ = j.at("when_clauses").get<std::vector<WhenClause>>();
    auto e2 = DeserializeExpression(j.at("default_expr"));
    default_expr_ = std::move(e2.result_);
    exprs.insert(exprs.end(), std::make_move_iterator(e2.non_owned_exprs_.begin()),
                 std::make_move_iterator(e2.non_owned_exprs_.end()));
    return exprs;
  }

 private:
  /** List of condition and result cases: WHEN ... THEN ... */
  std::vector<WhenClause> when_clauses_;
  /** Default result case. */
  std::unique_ptr<AbstractExpression> default_expr_;
};

DEFINE_JSON_DECLARATIONS(CaseExpression::WhenClause);
DEFINE_JSON_DECLARATIONS(CaseExpression);

}  // namespace terrier::parser
