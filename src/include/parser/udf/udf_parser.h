#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/ast/udf/udf_ast_context.h"
#include "execution/ast/udf/udf_ast_nodes.h"

#include "parser/expression_util.h"
#include "parser/postgresparser.h"

namespace noisepage {

namespace execution::ast::udf {
class FunctionAST;
}

namespace parser::udf {

/**
 * Namespace alias to make below more manageable.
 */
namespace udfexec = execution::ast::udf;

/**
 * The PLpgSQLParser class parses source PL/pgSQL to an abstract syntax tree.
 */
class PLpgSQLParser {
 public:
  /**
   * Construct a new PLpgSQLParser instance.
   * @param udf_ast_context The AST context
   * @param accessor The accessor to use during parsing
   * @param db_oid The database OID
   */
  PLpgSQLParser(common::ManagedPointer<udfexec::UdfAstContext> udf_ast_context,
                const common::ManagedPointer<catalog::CatalogAccessor> accessor, catalog::db_oid_t db_oid)
      : udf_ast_context_(udf_ast_context), accessor_(accessor), db_oid_(db_oid) {}

  /**
   * Parse source PL/pgSQL to an abstract syntax tree.
   * @param param_names The names of the function parameters
   * @param param_types The types of the function parameters
   * @param func_body The input source for the function
   * @param ast_context The AST context to use during parsing
   * @return The abstract syntax tree for the source function
   */
  std::unique_ptr<udfexec::FunctionAST> Parse(std::vector<std::string> &&param_names,
                                              std::vector<type::TypeId> &&param_types, const std::string &func_body,
                                              common::ManagedPointer<udfexec::UdfAstContext> ast_context);

 private:
  /**
   * Parse a block statement.
   * @param block The input JSON object
   * @return The AST for the block
   */
  std::unique_ptr<udfexec::StmtAST> ParseBlock(const nlohmann::json &block);

  /**
   * Parse a function statement.
   * @param block The input JSON object
   * @return The AST for the function
   */
  std::unique_ptr<udfexec::StmtAST> ParseFunction(const nlohmann::json &function);

  /**
   * Parse a declaration statement.
   * @param decl The input JSON object
   * @return The AST for the declaration
   */
  std::unique_ptr<udfexec::StmtAST> ParseDecl(const nlohmann::json &decl);

  /**
   * Parse an if-statement.
   * @param block The input JSON object
   * @return The AST for the if-statement
   */
  std::unique_ptr<udfexec::StmtAST> ParseIf(const nlohmann::json &branch);

  /**
   * Parse a while-statement.
   * @param block The input JSON object
   * @return The AST for the while-statement
   */
  std::unique_ptr<udfexec::StmtAST> ParseWhile(const nlohmann::json &loop);

  /**
   * Parse a for-statement.
   * @param block The input JSON object
   * @return The AST for the for-statement
   */
  std::unique_ptr<udfexec::StmtAST> ParseFor(const nlohmann::json &loop);

  /**
   * Parse a SQL statement.
   * @param sql_stmt The input JSON object
   * @return The AST for the SQL statement
   */
  std::unique_ptr<udfexec::StmtAST> ParseSQL(const nlohmann::json &sql_stmt);

  /**
   * Parse a dynamic SQL statement.
   * @param block The input JSON object
   * @return The AST for the dynamic SQL statement
   */
  std::unique_ptr<udfexec::StmtAST> ParseDynamicSQL(const nlohmann::json &sql_stmt);

  /**
   * Parse a SQL expression.
   * @param sql The SQL expression string
   * @return The AST for the SQL expression
   */
  std::unique_ptr<udfexec::ExprAST> ParseExprSQL(const std::string &sql);

  /**
   * Parse an expression.
   * @param expr The expression
   * @return The AST for the expression
   */
  std::unique_ptr<udfexec::ExprAST> ParseExpr(common::ManagedPointer<parser::AbstractExpression> expr);

 private:
  /** The UDF AST context */
  common::ManagedPointer<udfexec::UdfAstContext> udf_ast_context_;

  /** The catalog accessor */
  const common::ManagedPointer<catalog::CatalogAccessor> accessor_;

  /** The OID for the database with which the function is associated */
  catalog::db_oid_t db_oid_;

  /** The function symbol table */
  std::unordered_map<std::string, type::TypeId> symbol_table_;
};

}  // namespace parser::udf
}  // namespace noisepage
