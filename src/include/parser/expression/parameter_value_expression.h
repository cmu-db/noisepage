#pragma once
#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier {
namespace parser {
namespace expression {

/**
 * Represents a parameter's offset in an expression.
 */
class ParameterValueExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new ParameterValueExpression with the given offset.
   * @param value_idx the offset of the parameter
   */
  explicit ParameterValueExpression(const uint32_t value_idx)
      : AbstractExpression(ExpressionType::VALUE_PARAMETER, type::TypeId::PARAMETER_OFFSET,
                           std::vector<std::unique_ptr<AbstractExpression>>()),
        value_idx_(value_idx) {}

  AbstractExpression *Copy() const override { return new ParameterValueExpression(value_idx_); }

  uint32_t GetValueIdx() { return value_idx_; }

 private:
  uint32_t value_idx_;
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
