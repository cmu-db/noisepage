#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/ast/udf/udf_ast_context.h"
#include "execution/ast/udf/udf_ast_nodes.h"

#include "parser/expression_util.h"
#include "parser/postgresparser.h"

namespace noisepage::execution::ast::udf {
class FunctionAST;
}  // namespace noisepage::execution::ast::udf

namespace noisepage::parser::udf {

/**
 * The PLpgSQLParser class parses source PL/pgSQL to an abstract syntax tree.
 *
 * Internally, PLpgSQLParser utilizes libpg_query to perform the actual parsing
 * of the input PL/pgSQL source, and then maps the representation from libpg_query
 * to our our internal representation that then proceeds through code generation.
 */
class PLpgSQLParser {
 public:
  /**
   * Construct a new PLpgSQLParser instance.
   * @param udf_ast_context The AST context
   */
  explicit PLpgSQLParser(common::ManagedPointer<execution::ast::udf::UdfAstContext> udf_ast_context)
      : udf_ast_context_{udf_ast_context} {}

  /**
   * Parse source PL/pgSQL to an abstract syntax tree.
   * @param param_names The names of the function parameters
   * @param param_types The types of the function parameters
   * @param func_body The input source for the function
   * @param ast_context The AST context to use during parsing
   * @return The abstract syntax tree for the source function
   */
  std::unique_ptr<execution::ast::udf::FunctionAST> Parse(const std::vector<std::string> &param_names,
                                                          const std::vector<type::TypeId> &param_types,
                                                          const std::string &func_body);

 private:
  /**
   * Parse a block statement.
   * @param json The input JSON object
   * @return The AST for the block
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseBlock(const nlohmann::json &json);

  /**
   * Parse a function statement.
   * @param json The input JSON object
   * @return json AST for the function
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseFunction(const nlohmann::json &json);

  /**
   * Parse a declaration statement.
   * @param json The input JSON object
   * @return The AST for the declaration
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseDecl(const nlohmann::json &json);

  /**
   * Parse an if-statement.
   * @param json The input JSON object
   * @return The AST for the if-statement
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseIf(const nlohmann::json &json);

  /**
   * Parse a while-statement.
   * @param json The input JSON object
   * @return The AST for the while-statement
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseWhile(const nlohmann::json &json);

  /**
   * Parse a for-statement (integer variant).
   * @param json The input JSON object
   * @return The AST for the for-statement
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseForI(const nlohmann::json &json);

  /**
   * Parse a for-statement (query variant).
   * @param json The input JSON object
   * @return The AST for the for-statement
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseForS(const nlohmann::json &json);

  /**
   * Parse a SQL statement.
   * @param json The input JSON object
   * @return The AST for the SQL statement
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseSQL(const nlohmann::json &json);

  /**
   * Parse a dynamic SQL statement.
   * @param json The input JSON object
   * @return The AST for the dynamic SQL statement
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseDynamicSQL(const nlohmann::json &json);

  /**
   * Parse a SQL expression to an expression AST.
   * @param sql The SQL expression string
   * @return The AST for the SQL expression
   */
  std::unique_ptr<execution::ast::udf::ExprAST> ParseExprFromSQL(const std::string &sql);

  /**
   * Parse an abstract expression to an expression AST.
   * @param expr The abstract expression
   * @return The AST for the expression
   */
  std::unique_ptr<execution::ast::udf::ExprAST> ParseExprFromAbstract(
      common::ManagedPointer<parser::AbstractExpression> expr);

 private:
  /**
   * Determine if all variables in `names` are declared in the function.
   * @param names The collection of variable identifiers
   * @return `true` if all variables are declared, `false` otherwise
   */
  bool AllVariablesDeclared(const std::vector<std::string> &names) const;

  /**
   * Determine if any of the variables in `names` refer to a RECORD type.
   * @param names The collection of variable identifiers
   * @return `true` if any of the variables in `names` refer
   * to a RECORD type previously declared, `false` otherwise
   */
  bool ContainsRecordType(const std::vector<std::string> &names) const;

  /**
   * Resolve a PL/pgSQL RECORD type from a SELECT statement.
   * @param parse_result The result of parsing the SQL query
   * @return The resolved record type
   */
  std::vector<std::pair<std::string, type::TypeId>> ResolveRecordType(const ParseResult *parse_result);

 private:
  /** The UDF AST context */
  common::ManagedPointer<execution::ast::udf::UdfAstContext> udf_ast_context_;
  /** The function symbol table */
  std::unordered_map<std::string, type::TypeId> symbol_table_;
};

}  // namespace noisepage::parser::udf
