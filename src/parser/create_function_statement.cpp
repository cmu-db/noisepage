#include "parser/create_function_statement.h"

#include "binder/sql_node_visitor.h"

namespace terrier::parser {
void CreateFunctionStatement::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
  v->Visit(common::ManagedPointer(this));
}
}  // namespace terrier::parser
