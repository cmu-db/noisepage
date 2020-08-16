#include "parser/insert_statement.h"

#include "binder/sql_node_visitor.h"

namespace terrier::parser {
void InsertStatement::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
  v->Visit(common::ManagedPointer(this));
}
}  // namespace terrier::parser
