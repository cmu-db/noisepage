#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace terrier {
namespace parser {

/**
 * @struct UpdateClause
 * @brief Represents "column = value" expressions
 */
class UpdateClause {
 public:
  UpdateClause(std::string column, std::unique_ptr<AbstractExpression> value)
      : column_(std::move(column)), value_(std::move(value)) {}
  ~UpdateClause() = default;

  const std::string column_;
  const std::unique_ptr<AbstractExpression> value_;
};

/**
 * @class UpdateStatement
 * @brief Represents "UPDATE"
 */
class UpdateStatement : public SQLStatement {
 public:
  UpdateStatement(std::unique_ptr<TableRef> table, std::vector<std::unique_ptr<UpdateClause>> updates,
                  std::unique_ptr<AbstractExpression> where)
      : SQLStatement(StatementType::UPDATE),
        table_(std::move(table)),
        updates_(std::move(updates)),
        where_(std::move(where)) {}

  UpdateStatement() : SQLStatement(StatementType::UPDATE), table_(nullptr), where_(nullptr) {}

  ~UpdateStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  // TODO(pakhtar/WAN): switch to char* instead of TableRef
  // obsolete comment?
  const std::unique_ptr<TableRef> table_;
  const std::vector<std::unique_ptr<UpdateClause>> updates_;
  const std::unique_ptr<AbstractExpression> where_ = nullptr;
};

}  // namespace parser
}  // namespace terrier
