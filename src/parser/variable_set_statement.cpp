#include "parser/variable_set_statement.h"

#include "binder/sql_node_visitor.h"

namespace terrier::parser {
void VariableSetStatement::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
  v->Visit(common::ManagedPointer(this));
}
}  // namespace terrier::parser
