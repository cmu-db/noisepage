#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"

namespace terrier::parser {

/**
 * AggregateExpression is only used for parsing, planning and optimizing.
 * TODO(WAN): how is it used? Check with William?
 */
class AggregateExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new aggregate expression.
   * @param type type of aggregate expression
   * @param children children to be added
   * @param distinct whether to eliminate duplicate values in aggregate function calculations
   */
  AggregateExpression(ExpressionType type, std::vector<std::unique_ptr<AbstractExpression>> &&children, bool distinct)
      : AbstractExpression(type, type::TypeId::INVALID, std::move(children)), distinct_(distinct) {}

  /** Default constructor for deserialization. */
  AggregateExpression() = default;

  /**
   * Creates a copy of the current AbstractExpression
   * @returns Copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    for (const auto &child : GetChildren()) {
      children.emplace_back(child->Copy());
    }
    return CopyWithChildren(std::move(children));
  }

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    auto expr = std::make_unique<AggregateExpression>(GetExpressionType(), std::move(children), IsDistinct());
    expr->SetMutableStateForCopy(*this);
    return expr;
  }

  common::hash_t Hash() const override {
    common::hash_t hash = AbstractExpression::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(distinct_));
    return hash;
  }

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const AggregateExpression &>(rhs);
    return IsDistinct() == other.IsDistinct();
  }

  /** @return true if we should eliminate duplicate values in aggregate function calculations */
  bool IsDistinct() const { return distinct_; }

  void DeriveReturnValueType() override {
    auto expr_type = this->GetExpressionType();
    switch (expr_type) {
      case ExpressionType::AGGREGATE_COUNT:
        this->SetReturnValueType(type::TypeId::INTEGER);
        break;
        // keep the type of the base
      case ExpressionType::AGGREGATE_MAX:
      case ExpressionType::AGGREGATE_MIN:
      case ExpressionType::AGGREGATE_SUM:
        TERRIER_ASSERT(this->GetChildrenSize() >= 1, "No column name given.");
        const_cast<parser::AbstractExpression *>(this->GetChild(0).Get())->DeriveReturnValueType();
        this->SetReturnValueType(this->GetChild(0)->GetReturnValueType());
        break;
      case ExpressionType::AGGREGATE_AVG:
        this->SetReturnValueType(type::TypeId::DECIMAL);
        break;
      default:
        throw PARSER_EXCEPTION(
            ("Not a valid aggregation expression type: " + std::to_string(static_cast<int>(expr_type))).c_str());
    }
  }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v,
              common::ManagedPointer<binder::BinderSherpa> sherpa) override {
    v->Visit(common::ManagedPointer(this), sherpa);
  }

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["distinct"] = distinct_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override {
    std::vector<std::unique_ptr<AbstractExpression>> exprs;
    auto e1 = AbstractExpression::FromJson(j);
    exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
    distinct_ = j.at("distinct").get<bool>();
    return exprs;
  }

 private:
  /** True if duplicate rows should be removed. */
  bool distinct_;
};

DEFINE_JSON_DECLARATIONS(AggregateExpression);

}  // namespace terrier::parser
