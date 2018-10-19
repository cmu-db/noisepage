#pragma once

#include <memory>
#include <vector>
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier {
namespace parser {

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
  explicit SubqueryExpression(parser::SelectStatement *subselect)
      : AbstractExpression(ExpressionType::ROW_SUBQUERY, type::TypeId::INVALID,
                           std::vector<std::shared_ptr<AbstractExpression>>()) {
    subselect_ = std::shared_ptr<parser::SelectStatement>(subselect);
  }

  std::unique_ptr<AbstractExpression> Copy() const override {
    // TODO(WAN): Previous codebase described as a hack, will we need a deep copy?
    return std::unique_ptr<AbstractExpression>(new SubqueryExpression(subselect_.get()));
  }

  /**
   * @return shared pointer to stored sub-select
   */
  std::shared_ptr<parser::SelectStatement> GetSubselect() { return subselect_; }

 private:
  std::shared_ptr<parser::SelectStatement> subselect_;
};

}  // namespace parser
}  // namespace terrier
