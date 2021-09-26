#include <string>

#include "common/macros.h"
#include "execution/ast/udf/node_types.h"

namespace noisepage::execution::ast::udf {

std::string NodeTypeToShortString(NodeType type) {
  switch (type) {
    case NodeType::VALUE_EXPR:
      return "VALUE_EXPR";
    case NodeType::IS_NULL_EXPR:
      return "IS_NULL_EXPR";
    case NodeType::VARIABLE_EXPR:
      return "VARIABLE_EXPR";
    case NodeType::MEMBER_EXPR:
      return "MEMBER_EXPR";
    case NodeType::BINARY_EXPR:
      return "BINARY_EXPR";
    case NodeType::CALL_EXPR:
      return "CALL_EXPR";
    case NodeType::SEQ_STMT:
      return "SEQ_STMT";
    case NodeType::DECL_STMT:
      return "DECL_STMT";
    case NodeType::IF_STMT:
      return "IF_STMT";
    case NodeType::FORI_STMT:
      return "FORI_STMT";
    case NodeType::FORS_STMT:
      return "FORS_STMT";
    case NodeType::WHILE_STMT:
      return "WHILE_STMT";
    case NodeType::RET_STMT:
      return "RET_STMT";
    case NodeType::ASSIGN_STMT:
      return "ASSIGN_STMT";
    case NodeType::SQL_STMT:
      return "SQL_STMT";
    case NodeType::DYNAMIC_SQL_STMT:
      return "DYNAMIC_SQL_STMT";
    case NodeType::FUNCTION:
      return "FUNCTION";
    default:
      NOISEPAGE_ASSERT(false, "Impossible node type");
  }
}

}  // namespace noisepage::execution::ast::udf
