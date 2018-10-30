#pragma once
#include <memory>
#include <vector>
#include "parser/sql_node_visitor.h"
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier::parser {

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
      : AbstractExpression(ExpressionType::VALUE_PARAMETER, type::TypeId::PARAMETER_OFFSET, {}),
        value_idx_(value_idx) {}

  std::unique_ptr<AbstractExpression> Copy() const override {
    return std::make_unique<ParameterValueExpression>(*this);
  }

  std::vector<std::shared_ptr<sql::SqlAbstractExpression>> Accept(SqlNodeVisitor *v) override { return v->Visit(this); }

  /**
   * @return offset in the expression
   */
  uint32_t GetValueIdx() { return value_idx_; }

 private:
  // TODO(Tianyu): Can we get a better name for this?
  uint32_t value_idx_;
};

}  // namespace terrier::parser
