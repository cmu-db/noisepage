#pragma once

#include "common/sql_node_visitor.h"
#include "parser/expression/parameter_value_expression.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

#include <algorithm>

namespace terrier {
namespace parser {

/**
 * Represents the SQL "PREPARE ..."
 */
class PrepareStatement : public SQLStatement {
 public:
  PrepareStatement(std::string name, std::unique_ptr<SQLStatement> query,
                   std::vector<std::unique_ptr<ParameterValueExpression>> placeholders)
      : SQLStatement(StatementType::PREPARE),
        name_(std::move(name)),
        query_(std::move(query)),
        placeholders_(std::move(placeholders)) {}

  ~PrepareStatement() override = default;

  /*

   * @param vector of placeholders that the parser found
   *
   * When setting the placeholders we need to make sure that they are in the
   *correct order.
   * To ensure that, during parsing we store the character position use that to
   *sort the list here.

  void setPlaceholders(std::vector<void *> ph) {
    for (void *e : ph) {
      if (e != NULL)
        placeholders.push_back(
            std::unique_ptr<expression::ParameterValueExpression>(
                (expression::ParameterValueExpression *)e));
    }
    // Sort by col-id
    std::sort(placeholders.begin(), placeholders.end(),
              [](const std::unique_ptr<expression::ParameterValueExpression> &i,
                 const std::unique_ptr<expression::ParameterValueExpression> &j)
                  -> bool { return (i->ival_ < j->ival_); });

    // Set the placeholder id on the Expr. This replaces the previously stored
    // column id
    for (uint i = 0; i < placeholders.size(); ++i) placeholders[i]->ival_ = i;
  }
   */

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string name_;
  const std::unique_ptr<SQLStatement> query_;
  const std::vector<std::unique_ptr<ParameterValueExpression>> placeholders_;
};

}  // namespace parser
}  // namespace terrier
