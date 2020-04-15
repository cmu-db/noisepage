#pragma once

#include "binder/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace terrier {
namespace parser {
/**
 * Not sure what this is for. Inherited from old codebase.
 */
class VariableSetStatement : public SQLStatement {
  // TODO(WAN): inherited from old codebase.
  // It was added to the parser to avoid connection error by Yuchen,
  // because JDBC on starting connection will send SET and require a response.
 public:
  VariableSetStatement() : SQLStatement(StatementType::VARIABLE_SET) {}
  ~VariableSetStatement() override = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }
};
}  // namespace parser
}  // namespace terrier
