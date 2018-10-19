#pragma once
#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier {
namespace parser {

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
                           std::vector<std::shared_ptr<AbstractExpression>>()),
        value_idx_(value_idx) {}

  std::unique_ptr<AbstractExpression> Copy() const override {
    return std::unique_ptr<AbstractExpression>(new ParameterValueExpression(value_idx_));
  }

  /**
   * @return offset in the expression
   */
  uint32_t GetValueIdx() { return value_idx_; }

 private:
  uint32_t value_idx_;
};

}  // namespace parser
}  // namespace terrier
