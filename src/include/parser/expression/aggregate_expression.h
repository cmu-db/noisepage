#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "common/exception.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"

namespace terrier::parser {

/**
 * An AggregateExpression is only used for parsing, planning and optimizing.
 */
class AggregateExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new aggregate expression.
   * @param type type of aggregate expression
   * @param children children to be added
   * @param distinct whether to eliminate duplicate values in aggregate function calculations
   */
  AggregateExpression(ExpressionType type, std::vector<std::shared_ptr<AbstractExpression>> &&children, bool distinct)
      : AbstractExpression(type, type::TypeId::INVALID, std::move(children)), distinct_(distinct) {}

  /**
   * Default constructor for deserialization
   */
  AggregateExpression() = default;

  std::shared_ptr<AbstractExpression> Copy() const override { return std::make_shared<AggregateExpression>(*this); }

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

  /**
   * @return true if we should eliminate duplicate values in aggregate function calculations
   */
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
        this->GetChild(0)->DeriveReturnValueType();
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

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override {
    nlohmann::json j = AbstractExpression::ToJson();
    j["distinct"] = distinct_;
    return j;
  }

  /**
   * @param j json to deserialize
   */
  void FromJson(const nlohmann::json &j) override {
    AbstractExpression::FromJson(j);
    distinct_ = j.at("distinct").get<bool>();
  }

 private:
  /**
   * If duplicate rows will be removed
   */
  bool distinct_;
};

DEFINE_JSON_DECLARATIONS(AggregateExpression);

}  // namespace terrier::parser
