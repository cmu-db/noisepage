#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/ast/udf/udf_ast_context.h"
#include "execution/ast/udf/udf_ast_nodes.h"

#include "parser/expression_util.h"
#include "parser/postgresparser.h"
#include "parser/sql_statement.h"

namespace noisepage::parser {
class SQLStatement;
}  // namespace noisepage::parser

namespace noisepage::execution::ast::udf {
class FunctionAST;
}  // namespace noisepage::execution::ast::udf

namespace noisepage::parser::udf {

/** An enumeration over the supported PL/pgSQL statement types */
enum class StatementType { UNKNOWN, RETURN, IF, ASSIGN, WHILE, FORI, FORS, EXECSQL, DYNEXECUTE };

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
   * Parse a return statement.
   * @param json The input JSON object
   * @return The AST for the return statement
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseReturn(const nlohmann::json &json);

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
   * Parse an assignment statement.
   * @param json The input JSON object
   * @return The AST for the assignment
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseAssign(const nlohmann::json &json);

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
  std::unique_ptr<execution::ast::udf::StmtAST> ParseExecSQL(const nlohmann::json &json);

  /**
   * Parse a SQL statement.
   * @param sql The input SQL query text
   * @param variables The collection of variables to which results are bound
   * @return The AST for the SQL statement
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseExecSQL(const std::string &sql,
                                                             std::vector<std::string> &&variables);

  /**
   * Parse a dynamic SQL statement.
   * @param json The input JSON object
   * @return The AST for the dynamic SQL statement
   */
  std::unique_ptr<execution::ast::udf::StmtAST> ParseDynamicSQL(const nlohmann::json &json);

  /**
   * Parse a SQL expression from a query string.
   * @param sql The SQL expression string
   * @return The AST for the SQL expression
   */
  std::unique_ptr<execution::ast::udf::ExprAST> ParseExprFromSQLString(const std::string &sql);

  /**
   * Try to parse a SQL expression from a query string. If the expression
   * type is not supported, indicate failure with an empty std::optional.
   * @param sql The SQL expression string
   * @return The AST for the SQL expression on success, empty std::optional on failure
   */
  std::optional<std::unique_ptr<execution::ast::udf::ExprAST>> TryParseExprFromSQLString(
      const std::string &sql) noexcept;

  /**
   * Parse a SQL expression from a SQL statement.
   * @param statement The SQL statement
   * @return The AST for the SQL statement
   */
  std::unique_ptr<execution::ast::udf::ExprAST> ParseExprFromSQLStatement(
      common::ManagedPointer<SQLStatement> statement);

  /**
   * Try to parse an abstract expression from a SQL statement. If the statement
   * type is not supported, indicate failure with an empty std::optional.
   * @param statement The input SQL statement
   * @return The AST for the statement on success, empty std::optional on failure
   */
  std::optional<std::unique_ptr<execution::ast::udf::ExprAST>> TryParseExprFromSQLStatement(
      common::ManagedPointer<SQLStatement> statement) noexcept;

  /**
   * Parse an abstract expression to an expression AST.
   * @param expr The abstract expression
   * @return The AST for the expression
   */
  std::unique_ptr<execution::ast::udf::ExprAST> ParseExprFromAbstract(
      common::ManagedPointer<parser::AbstractExpression> expr);

  /**
   * Try to parse an abstract expression to an expression AST. If the expression
   * type is not supported, indicate failure with an empty std::optional.
   * @param expr The input expression
   * @return The AST for the expression on success, empty std::optional on failure
   */
  std::optional<std::unique_ptr<execution::ast::udf::ExprAST>> TryParseExprFromAbstract(
      common::ManagedPointer<parser::AbstractExpression> expr) noexcept;

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

  /**
   * Get the StatementType for the provided statement type identifier.
   * @param type The identifier for the statement type
   * @return The corresponding StatementType
   */
  static StatementType GetStatementType(const std::string &type);

  /**
   * Strip an enclosing SELECT query from an existing ParseResult.
   * @param input The existing ParseResult
   * @return A new ParseResult with the enclosing query stripped
   */
  static std::unique_ptr<parser::ParseResult> StripEnclosingQuery(std::unique_ptr<ParseResult> &&input);

  /**
   * Determine if the parsed query has an enclosing "wrapper" query
   * introduced by the PL/pgSQL parser.
   * @param parse_result The parsed query
   * @return `true` if the query has an enclosing query, `false` otherwise
   */
  static bool HasEnclosingQuery(ParseResult *parse_result);

 private:
  /** The UDF AST context */
  common::ManagedPointer<execution::ast::udf::UdfAstContext> udf_ast_context_;
  /** The function symbol table */
  std::unordered_map<std::string, type::TypeId> symbol_table_;
};

}  // namespace noisepage::parser::udf
