#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier::parser {

class SelectStatement {};  // TODO(WAN): temporary until we get a real parser - why is it a parser class?

/**
 * Represents a sub-select query.
 */
class SubqueryExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new SubqueryExpression with the given sub-select from the parser.
   * @param subselect the sub-select
   */
  explicit SubqueryExpression(std::shared_ptr<parser::SelectStatement> subselect)
      : AbstractExpression(ExpressionType::ROW_SUBQUERY, type::TypeId::INVALID, {}), subselect_(std::move(subselect)) {}

  std::unique_ptr<AbstractExpression> Copy() const override {
    // TODO(WAN): Previous codebase described as a hack, will we need a deep copy?
    // Tianyu: No need for deep copy if your objects are always immutable! (why even copy at all, but that's beyond me)
    return std::make_unique<SubqueryExpression>(*this);
  }

  std::shared_ptr<sql::SqlAbstractExpression> Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return shared pointer to stored sub-select
   */
  std::shared_ptr<parser::SelectStatement> GetSubselect() { return subselect_; }

 private:
  std::shared_ptr<parser::SelectStatement> subselect_;
};

}  // namespace terrier::parser
