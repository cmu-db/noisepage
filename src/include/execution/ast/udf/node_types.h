namespace noisepage::execution::ast::udf {

/** Enumerates all (instantiable) AST node types */
enum class NodeType {
  VALUE_EXPR,
  IS_NULL_EXPR,
  VARIABLE_EXPR,
  MEMBER_EXPR,
  BINARY_EXPR,
  CALL_EXPR,
  SEQ_STMT,
  DECL_STMT,
  IF_STMT,
  FORI_STMT,
  FORS_STMT,
  WHILE_STMT,
  RET_STMT,
  ASSIGN_STMT,
  SQL_STMT,
  DYNAMIC_SQL_STMT,
  FUNCTION
};

}  // namespace noisepage::execution::ast::udf
