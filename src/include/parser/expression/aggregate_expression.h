#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/error/exception.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"

namespace noisepage::parser {

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
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override;

  common::hash_t Hash() const override;

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const AggregateExpression &>(rhs);
    return IsDistinct() == other.IsDistinct();
  }

  /** @return true if we should eliminate duplicate values in aggregate function calculations */
  bool IsDistinct() const { return distinct_; }

  /**
   * Derive the expression type of the current expression.
   */
  void DeriveReturnValueType() override;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override;

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  /** True if duplicate rows should be removed. */
  bool distinct_;
};

DEFINE_JSON_HEADER_DECLARATIONS(AggregateExpression);

}  // namespace noisepage::parser
