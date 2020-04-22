#pragma once


#include "binder/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace terrier::parser {
/**
 * @class AlterTableStatement
 * @brief
 */

// TODO(SC)
class AlterTableStatement : public SQLStatement {
 public:
  // TODO(SC)
  AlterTableStatement(std::unique_ptr<TableRef> table)
      : SQLStatement(StatementType::ALTER), table_ref_(std::move(table)){}

  // TODO(SC)
  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

 private:
  const std::unique_ptr<TableRef> table_ref_;
};

}  // namespace terrier::parser
