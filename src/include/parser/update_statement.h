#pragma once

#include <cstring>

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
  std::string column;
  std::unique_ptr<AbstractExpression> value;

  ~UpdateClause() {}

/*  UpdateClause *Copy() {
    UpdateClause *new_clause = new UpdateClause();
    std::string str(column);
    new_clause->column = column;
    AbstractExpression *new_expr = value->Copy();
    new_clause->value.reset(new_expr);
    return new_clause;
  }*/
};

/**
 * @class UpdateStatement
 * @brief Represents "UPDATE"
 */
class UpdateStatement : public SQLStatement {
 public:
  UpdateStatement(std::unique_ptr<TableRef> table,
		  std::vector<std::unique_ptr<UpdateClause>> updates,
		  std::unique_ptr<AbstractExpression> where)
    : SQLStatement(StatementType::UPDATE),
      table_(std::move(table)),
      updates_(std::move(updates)),
      where_(std::move(where)) {}
  
  UpdateStatement()
      : SQLStatement(StatementType::UPDATE), table_(nullptr), where_(nullptr) {}

  virtual ~UpdateStatement() {}

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  //const std::string GetInfo(int num_indent) const override;
  //const std::string GetInfo() const override;

  // TODO: switch to char* instead of TableRef
  // obsolete comment?
  std::unique_ptr<TableRef> table_;
  std::vector<std::unique_ptr<UpdateClause>> updates_;
  std::unique_ptr<AbstractExpression> where_ = nullptr;
};

}  // namespace parser
}  // namespace terrier
