#pragma once

#include <memory>
#include <utility>
#include <vector>
#include "sql/expression/sql_abstract_expression.h"
#include "type/type_id.h"

namespace terrier::sql {

class SelectStatement {};  // TODO(WAN): temporary until we get a real parser - why is it a parser class?

/**
 * Represents a sub-select query.
 */
class SqlSubqueryExpression : public SqlAbstractExpression {
 public:
  /**
   * Instantiates a new SubqueryExpression with the given sub-select from the parser.
   * @param subselect the sub-select
   */
  explicit SqlSubqueryExpression(std::shared_ptr<sql::SelectStatement> subselect)
      : SqlAbstractExpression(parser::ExpressionType::ROW_SUBQUERY, type::TypeId::INVALID, {}), subselect_(std::move(subselect)) {}

  std::unique_ptr<SqlAbstractExpression> Copy() const override {
    return std::make_unique<SqlSubqueryExpression>(*this);
  }

  /**
   * @return shared pointer to stored sub-select
   */
  std::shared_ptr<sql::SelectStatement> GetSubselect() { return subselect_; }

 private:
  std::shared_ptr<sql::SelectStatement> subselect_;
};

}  // namespace terrier::sql
