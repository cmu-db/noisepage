#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "common/macros.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"

namespace terrier::parser {

/**
 * Represents a logical case expression.
 */
class CaseExpression : public AbstractExpression {
 public:
  /**
   * WHEN ... THEN ... clauses.
   */
  class WhenClause {
   public:
    /**
     * @param condition condition to be met
     * @param then action when condition is met
     */
    WhenClause(AbstractExpression *condition, AbstractExpression *then) : condition_(condition), then_(then) {
      TERRIER_ASSERT(condition != nullptr, "Condition for case expression can't be null");
      TERRIER_ASSERT(then != nullptr, "Then for case expression can't be null");
    }

    /**
     * Default constructor used for deserialization
     */
    WhenClause() = default;

    ~WhenClause() {
      delete condition_;
      delete then_;
    }

    /**
     * Copy constructor
     * @param other WhenClause to copy from
     */
    WhenClause(const WhenClause &other) {
      condition_ = other.condition_->Copy();
      then_ = other.then_ == nullptr ? nullptr : other.then_->Copy();
    }

    /**
     * Copy assignment operator
     * @param other WhenClause to copy from
     * @return self reference
     */
    WhenClause &operator=(const WhenClause &other) {
      condition_ = other.condition_->Copy();
      then_ = other.then_ == nullptr ? nullptr : other.then_->Copy();
      return *this;
    }

    /**
     * Move Constructor
     * @param from WhenClause to be moved from
     * @warning WhenClause from will be left with a null expression.
     */
    WhenClause(WhenClause &&from) noexcept {
      condition_ = from.condition_;
      then_ = from.then_;
      from.condition_ = nullptr;
      from.then_ = nullptr;
    }

    /**
     * Move assignment operator
     * @param from WhenClause to be moved from
     * @return self reference
     * @warning WhenClause from will be left with a null expression.
     */
    WhenClause &operator=(WhenClause &&from) noexcept {
      if (this == &from) {
        return *this;
      }
      condition_ = from.condition_;
      then_ = from.then_;
      from.condition_ = nullptr;
      from.then_ = nullptr;
      return *this;
    }

    /**
     * The condition to be checked for this case expression.
     */
    AbstractExpression *condition_;
    /**
     * The value that this expression should have if the corresponding condition is true.
     */
    AbstractExpression *then_;

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
      hash = common::HashUtil::CombineHashes(hash, then_->Hash());
      return hash;
    }

    /**
     * Derived expressions should call this base method
     * @return expression serialized to json
     */
    nlohmann::json ToJson() const {
      nlohmann::json j;
      j["condition"] = condition_;
      j["then"] = then_;
      return j;
    }

    /**
     * Derived expressions should call this base method
     * @param j json to deserialize
     */
    void FromJson(const nlohmann::json &j) {
      condition_ = DeserializeExpression(j.at("condition"));
      then_ = DeserializeExpression(j.at("then"));
    }
  };

  /**
   * Instantiate a new case expression.
   * @param return_value_type return value of the case expression
   * @param when_clauses list of WhenClauses
   * @param default_expr default expression for this case
   */
  CaseExpression(const type::TypeId return_value_type, std::vector<WhenClause> when_clauses,
                 AbstractExpression *default_expr)
      : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR, return_value_type, {}),
        when_clauses_(std::move(when_clauses)),
        default_expr_(default_expr) {}

  /**
   * Default constructor for deserialization
   */
  CaseExpression() = default;

  ~CaseExpression() override { delete default_expr_; }

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

  AbstractExpression *Copy() const override {
    return new CaseExpression(GetReturnValueType(), when_clauses_, default_expr_->Copy());
  }

  /**
   * @return the number of WhenClauses
   */
  size_t GetWhenClauseSize() const { return when_clauses_.size(); }

  /**
   * @param index index of WhenClause to get
   * @return condition at that index
   */
  AbstractExpression *GetWhenClauseCondition(size_t index) const {
    TERRIER_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return when_clauses_[index].condition_;
  }

  /**
   * @param index index of WhenClause to get
   * @return result at that index
   */
  AbstractExpression *GetWhenClauseResult(size_t index) const {
    TERRIER_ASSERT(index < when_clauses_.size(), "Index must be in bounds.");
    return when_clauses_[index].then_;
  }

  /**
   * @return default clause, if it exists
   */
  AbstractExpression *GetDefaultClause() const { return default_expr_; }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["when_clauses"] = when_clauses_;
    j["default_expr"] = default_expr_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    when_clauses_ = j.at("when_clauses").get<std::vector<WhenClause>>();
    default_expr_ = DeserializeExpression(j.at("default_expr"));
  }

 private:
  std::vector<WhenClause> when_clauses_;
  AbstractExpression *default_expr_;
};

DEFINE_JSON_DECLARATIONS(CaseExpression::WhenClause);
DEFINE_JSON_DECLARATIONS(CaseExpression);

}  // namespace terrier::parser
