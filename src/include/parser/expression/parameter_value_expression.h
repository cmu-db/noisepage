#pragma once

#include <memory>
#include <vector>

#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace noisepage::parser {
/**
 * ParameterValueExpression represents a parameter's offset in an expression.
 * TODO(WAN): give an example. I believe this is 0-indexed, look at ParamRefTransform code path. Good beginner task?
 */
class ParameterValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new ParameterValueExpression with the given offset.
   * @param value_idx the offset of the parameter
   * @warning we set the type to INVALID as an indicator to the binder that we have not visited this expression yet.
   * After being visited by the binder, the type should reflect the correct value
   */
  explicit ParameterValueExpression(const uint32_t value_idx)
      : AbstractExpression(ExpressionType::VALUE_PARAMETER, type::TypeId::INVALID, {}), value_idx_(value_idx) {}

  /**
   * Instantiates a new ParameterValueExpression with the given offset and the given type
   * @param value_idx the offset of the parameter
   * @param ret_type the return type of the expression
   */
  explicit ParameterValueExpression(const uint32_t value_idx, type::TypeId ret_type)
      : AbstractExpression(ExpressionType::VALUE_PARAMETER, ret_type, {}), value_idx_(value_idx) {}

  /** Default constructor for deserialization. */
  ParameterValueExpression() = default;

  /**
   * Copies this ParameterValueExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Creates a copy of the current AbstractExpression with new children implanted.
   * The children should not be owned by any other AbstractExpression.
   * @param children New children to be owned by the copy
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    NOISEPAGE_ASSERT(children.empty(), "ParameterValueExpression should have 0 children");
    return Copy();
  }

  /** @return offset in the expression */
  uint32_t GetValueIdx() const { return value_idx_; }

  common::hash_t Hash() const override;

  bool operator==(const AbstractExpression &rhs) const override {
    if (!AbstractExpression::operator==(rhs)) return false;
    auto const &other = dynamic_cast<const ParameterValueExpression &>(rhs);
    return GetValueIdx() == other.GetValueIdx();
  }

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override;

  /** @return expression serialized to json */
  nlohmann::json ToJson() const override;

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  // TODO(Tianyu): Can we get a better name for this?
  /** Offset of the value that this expression points to in the query's parameter list. */
  uint32_t value_idx_;
};

DEFINE_JSON_HEADER_DECLARATIONS(ParameterValueExpression);

}  // namespace noisepage::parser
