#pragma once

#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier {
namespace parser {
class SelectStatement {};  // TODO(WAN): temporary until we get a real parser - why is it a parser class?
}  // namespace parser

namespace parser {
namespace expression {

/**
 * Represents a sub-select query.
 */
class SubqueryExpression : public AbstractExpression {
 public:
  /**
   * Instantiates a new SubqueryExpression with the given sub-select from the parser.
   * @param subselect the sub-select
   */
  explicit SubqueryExpression(const parser::SelectStatement *subselect)
      : AbstractExpression(ExpressionType::ROW_SUBQUERY, type::TypeId::INVALID,
                           std::vector<std::unique_ptr<AbstractExpression>>()) {
    subselect_ = std::make_shared<parser::SelectStatement>(*subselect);
  }

  AbstractExpression *Copy() const override {
    // TODO(WAN): Previous codebase described as a hack, will we need a deep copy?
    return new SubqueryExpression(subselect_.get());
  }

  /**
   * Return a shared pointer to the stored sub-select.
   * @return shared pointer to stored sub-select
   */
  std::shared_ptr<parser::SelectStatement> GetSubselect() { return subselect_; }

 private:
  std::shared_ptr<parser::SelectStatement> subselect_;
};

}  // namespace expression
}  // namespace parser
}  // namespace terrier
