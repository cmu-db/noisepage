#include <sstream>

#include "binder/bind_node_visitor.h"
#include "execution/ast/udf/udf_ast_nodes.h"
#include "parser/expression/subquery_expression.h"
#include "parser/udf/plpgsql_parse_result.h"
#include "parser/udf/plpgsql_parser.h"
#include "parser/udf/string_utils.h"

#include "libpg_query/pg_query.h"
#include "nlohmann/json.hpp"

namespace noisepage::parser::udf {

/** The identifiers used as keys in the parse tree */
static constexpr const char K_FUNCTION_LIST[] = "FunctionList";
static constexpr const char K_DATUMS[] = "datums";
static constexpr const char K_PLPGSQL_VAR[] = "PLpgSQL_var";
static constexpr const char K_REFNAME[] = "refname";
static constexpr const char K_DATATYPE[] = "datatype";
static constexpr const char K_DEFAULT_VAL[] = "default_val";
static constexpr const char K_PLPGSQL_TYPE[] = "PLpgSQL_type";
static constexpr const char K_TYPENAME[] = "typname";
static constexpr const char K_ACTION[] = "action";
static constexpr const char K_PLPGSQL_FUNCTION[] = "PLpgSQL_function";
static constexpr const char K_BODY[] = "body";
static constexpr const char K_PLPGSQL_STMT_BLOCK[] = "PLpgSQL_stmt_block";
static constexpr const char K_PLPGSQL_STMT_RETURN[] = "PLpgSQL_stmt_return";
static constexpr const char K_PLPGSQL_STMT_IF[] = "PLpgSQL_stmt_if";
static constexpr const char K_PLPGSQL_STMT_WHILE[] = "PLpgSQL_stmt_while";
static constexpr const char K_PLPGSQL_STMT_FORS[] = "PLpgSQL_stmt_fors";
static constexpr const char K_PLPGSQL_STMT_FORI[] = "PLpgSQL_stmt_fori";
static constexpr const char K_COND[] = "cond";
static constexpr const char K_THEN_BODY[] = "then_body";
static constexpr const char K_ELSE_BODY[] = "else_body";
static constexpr const char K_EXPR[] = "expr";
static constexpr const char K_QUERY[] = "query";
static constexpr const char K_PLPGSQL_EXPR[] = "PLpgSQL_expr";
static constexpr const char K_PLPGSQL_STMT_ASSIGN[] = "PLpgSQL_stmt_assign";
static constexpr const char K_VARNO[] = "varno";
static constexpr const char K_PLGPSQL_STMT_EXECSQL[] = "PLpgSQL_stmt_execsql";
static constexpr const char K_SQLSTMT[] = "sqlstmt";
static constexpr const char K_ROW[] = "row";
static constexpr const char K_FIELDS[] = "fields";
static constexpr const char K_NAME[] = "name";
static constexpr const char K_PLPGSQL_ROW[] = "PLpgSQL_row";
static constexpr const char K_PLPGSQL_STMT_DYNEXECUTE[] = "PLpgSQL_stmt_dynexecute";
static constexpr const char K_LOWER[] = "lower";
static constexpr const char K_UPPER[] = "upper";
static constexpr const char K_STEP[] = "step";
static constexpr const char K_VAR[] = "var";

/** Variable declaration type identifiers */
static constexpr const char DECL_TYPE_ID_INT[] = "int";
static constexpr const char DECL_TYPE_ID_INTEGER[] = "integer";
static constexpr const char DECL_TYPE_ID_DOUBLE[] = "double";
static constexpr const char DECL_TYPE_ID_NUMERIC[] = "numeric";
static constexpr const char DECL_TYPE_ID_VARCHAR[] = "varchar";
static constexpr const char DECL_TYPE_ID_DATE[] = "date";
static constexpr const char DECL_TYPE_ID_RECORD[] = "record";

std::unique_ptr<execution::ast::udf::FunctionAST> PLpgSQLParser::Parse(const std::vector<std::string> &param_names,
                                                                       const std::vector<type::TypeId> &param_types,
                                                                       const std::string &func_body) {
  auto result = PLpgSQLParseResult{pg_query_parse_plpgsql(func_body.c_str())};
  if ((*result).error != nullptr) {
    throw PARSER_EXCEPTION(fmt::format("PL/pgSQL parser : {}", (*result).error->message));
  }

  // The result is a list, we need to wrap it
  const auto ast_json_str = fmt::format("{{ \"{}\" : {} }}", K_FUNCTION_LIST, (*result).plpgsql_funcs);

  const nlohmann::json ast_json = nlohmann::json::parse(ast_json_str);
  const auto function_list = ast_json[K_FUNCTION_LIST];
  NOISEPAGE_ASSERT(function_list.is_array(), "Function list is not an array");

  if (function_list.size() != 1) {
    throw PARSER_EXCEPTION("Function list has size other than 1");
  }

  // TODO(Kyle): This is a zip(), can we add our own generic
  // algorithms library somewhere for stuff like this?
  std::size_t i = 0;
  for (const auto &udf_name : param_names) {
    udf_ast_context_->SetVariableType(udf_name, param_types[i++]);
  }
  const auto function = function_list[0][K_PLPGSQL_FUNCTION];
  return std::make_unique<execution::ast::udf::FunctionAST>(ParseFunction(function), param_names, param_types);
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseFunction(const nlohmann::json &json) {
  const auto declarations = json[K_DATUMS];
  NOISEPAGE_ASSERT(declarations.is_array(), "Declaration list is not an array");

  const auto function_body = json[K_ACTION][K_PLPGSQL_STMT_BLOCK][K_BODY];

  std::vector<std::unique_ptr<execution::ast::udf::StmtAST>> statements{};
  // Skip the first declaration in the datums list
  std::transform(declarations.cbegin() + 1, declarations.cend(), std::back_inserter(statements),
                 [this](const nlohmann::json &declaration) -> std::unique_ptr<execution::ast::udf::StmtAST> {
                   return ParseDecl(declaration);
                 });
  statements.push_back(ParseBlock(function_body));
  return std::make_unique<execution::ast::udf::SeqStmtAST>(std::move(statements));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseBlock(const nlohmann::json &json) {
  NOISEPAGE_ASSERT(json.is_array(), "Block isn't array");
  if (json.empty()) {
    throw PARSER_EXCEPTION("PL/pgSQL parser : Empty block is not supported");
  }

  std::vector<std::unique_ptr<execution::ast::udf::StmtAST>> statements{};
  for (const auto &statement : json) {
    const StatementType statement_type = GetStatementType(statement.items().begin().key());
    switch (statement_type) {
      case StatementType::RETURN: {
        // TODO(Kyle): Handle RETURN without expression
        if (statement[K_PLPGSQL_STMT_RETURN].empty()) {
          throw NOT_IMPLEMENTED_EXCEPTION("PL/pgSQL Parser : RETURN without expression not implemented.");
        }
        auto expr = ParseExprFromSQLString(
            statement[K_PLPGSQL_STMT_RETURN][K_EXPR][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
        statements.push_back(std::make_unique<execution::ast::udf::RetStmtAST>(std::move(expr)));
        break;
      }
      case StatementType::IF: {
        statements.push_back(ParseIf(statement[K_PLPGSQL_STMT_IF]));
        break;
      }
      case StatementType::ASSIGN: {
        // TODO(Kyle): Need to fix Assignment expression / statement
        statements.push_back(ParseAssign(statement[K_PLPGSQL_STMT_ASSIGN]));
        break;
      }
      case StatementType::WHILE: {
        statements.push_back(ParseWhile(statement[K_PLPGSQL_STMT_WHILE]));
        break;
      }
      case StatementType::FORI: {
        statements.push_back(ParseForI(statement[K_PLPGSQL_STMT_FORI]));
        break;
      }
      case StatementType::FORS: {
        statements.push_back(ParseForS(statement[K_PLPGSQL_STMT_FORS]));
        break;
      }
      case StatementType::EXECSQL: {
        statements.push_back(ParseExecSQL(statement[K_PLGPSQL_STMT_EXECSQL]));
        break;
      }
      case StatementType::DYNEXECUTE: {
        statements.push_back(ParseDynamicSQL(statement[K_PLPGSQL_STMT_DYNEXECUTE]));
        break;
      }
      case StatementType::UNKNOWN: {
        throw PARSER_EXCEPTION(
            fmt::format("PL/pgSQL Parser : statement type '{}' not supported", statement.items().begin().key()));
      }
    }
  }

  return std::make_unique<execution::ast::udf::SeqStmtAST>(std::move(statements));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseDecl(const nlohmann::json &json) {
  const auto &declaration_type = json.items().begin().key();
  if (declaration_type == K_PLPGSQL_VAR) {
    auto var_name = json[K_PLPGSQL_VAR][K_REFNAME].get<std::string>();

    // Track the local variable (for assignment)
    udf_ast_context_->AddLocal(var_name);

    // Grab the type identifier from the PL/pgSQL parser
    const std::string type = StringUtils::Strip(
        StringUtils::Lower(json[K_PLPGSQL_VAR][K_DATATYPE][K_PLPGSQL_TYPE][K_TYPENAME].get<std::string>()));

    // Parse the initializer, if present
    std::unique_ptr<execution::ast::udf::ExprAST> initial{nullptr};
    if (json[K_PLPGSQL_VAR].find(K_DEFAULT_VAL) != json[K_PLPGSQL_VAR].end()) {
      initial = ParseExprFromSQLString(json[K_PLPGSQL_VAR][K_DEFAULT_VAL][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
    }

    // Detemine if the variable has already been declared;
    // if so, just re-use this type that has already been resolved
    const auto resolved_type = udf_ast_context_->GetVariableType(var_name);
    if (resolved_type.has_value()) {
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, resolved_type.value(), std::move(initial));
    }

    // Otherwise, we perform a string comparison with the type identifier
    // for the variable to determine the type for the declaration

    if ((type == DECL_TYPE_ID_INT) || (type == DECL_TYPE_ID_INTEGER)) {
      udf_ast_context_->SetVariableType(var_name, type::TypeId::INTEGER);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::INTEGER, std::move(initial));
    }
    if ((type == DECL_TYPE_ID_DOUBLE) || (type == DECL_TYPE_ID_NUMERIC)) {
      // TODO(Kyle): type.rfind("numeric")
      // TODO(Kyle): Should this support FLOAT and DECMIAL as well??
      udf_ast_context_->SetVariableType(var_name, type::TypeId::DECIMAL);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::DECIMAL, std::move(initial));
    }
    if (type == DECL_TYPE_ID_VARCHAR) {
      udf_ast_context_->SetVariableType(var_name, type::TypeId::VARCHAR);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::VARCHAR, std::move(initial));
    }
    if (type == DECL_TYPE_ID_DATE) {
      udf_ast_context_->SetVariableType(var_name, type::TypeId::DATE);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::DATE, std::move(initial));
    }
    if (type == DECL_TYPE_ID_RECORD) {
      udf_ast_context_->SetVariableType(var_name, type::TypeId::INVALID);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::INVALID, std::move(initial));
    }

    throw PARSER_EXCEPTION(fmt::format("PL/pgSQL Parser : unsupported type '{}' for variable '{}'", type, var_name));
  }

  // TODO(Kyle): Support row types later
  if (declaration_type == K_PLPGSQL_ROW) {
    const auto var_name = json[K_PLPGSQL_ROW][K_REFNAME].get<std::string>();
    NOISEPAGE_ASSERT(var_name == "*internal*", "Unexpected refname");
    udf_ast_context_->SetVariableType(var_name, type::TypeId::INVALID);
    return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::INVALID, nullptr);
  }

  // TODO(Kyle): Need to handle other types like row, table etc;
  throw PARSER_EXCEPTION(fmt::format("PL/pgSQL Parser : declaration type '{}' not supported", declaration_type));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseAssign(const nlohmann::json &json) {
  // Extract the destination of the assignment
  const auto var_index = json[K_VARNO].get<std::size_t>() - 1;
  const auto &var_name = udf_ast_context_->GetLocalAtIndex(var_index);

  // Attempt to parse the SQL expression to an AST expression
  const auto &sql = json[K_EXPR][K_PLPGSQL_EXPR][K_QUERY].get<std::string>();
  auto rhs = TryParseExprFromSQLString(sql);
  if (rhs.has_value()) {
    auto lhs = std::make_unique<execution::ast::udf::VariableExprAST>(var_name);
    return std::make_unique<execution::ast::udf::AssignStmtAST>(std::move(lhs), std::move(*rhs));
  }

  // Failed to parse the SQL expression to an AST expression;
  // this could be the result of malformed SQL, OR it could
  // be that the SQL is sufficiently complex that we need to
  // generate code to execute the query. In this latter case,
  // we use the existing infrastructure for executing SQL in
  // the UDF body, and "desugar" the assignment to a SELECT INTO.

  // TODO(Kyle): Is this semantically correct? We are hacking
  // an assignment expression into a SQL execution statement
  return ParseExecSQL(sql, std::vector<std::string>{var_name});
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseIf(const nlohmann::json &json) {
  auto cond_expr = ParseExprFromSQLString(json[K_COND][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
  auto then_stmt = ParseBlock(json[K_THEN_BODY]);
  std::unique_ptr<execution::ast::udf::StmtAST> else_stmt =
      json.contains(K_ELSE_BODY) ? ParseBlock(json[K_ELSE_BODY]) : nullptr;
  return std::make_unique<execution::ast::udf::IfStmtAST>(std::move(cond_expr), std::move(then_stmt),
                                                          std::move(else_stmt));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseWhile(const nlohmann::json &json) {
  auto cond_expr = ParseExprFromSQLString(json[K_COND][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
  auto body_stmt = ParseBlock(json[K_BODY]);
  return std::make_unique<execution::ast::udf::WhileStmtAST>(std::move(cond_expr), std::move(body_stmt));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseForI(const nlohmann::json &json) {
  const auto name = json[K_VAR][K_PLPGSQL_VAR][K_REFNAME].get<std::string>();
  auto lower = ParseExprFromSQLString(json[K_LOWER][K_PLPGSQL_EXPR][K_QUERY]);
  auto upper = ParseExprFromSQLString(json[K_UPPER][K_PLPGSQL_EXPR][K_QUERY]);
  auto step = json.contains(K_STEP) ? ParseExprFromSQLString(json[K_STEP][K_PLPGSQL_EXPR][K_QUERY])
                                    : ParseExprFromSQLString(execution::ast::udf::ForIStmtAST::DEFAULT_STEP_EXPR);
  auto body = ParseBlock(json[K_BODY]);
  return std::make_unique<execution::ast::udf::ForIStmtAST>(name, std::move(lower), std::move(upper), std::move(step),
                                                            std::move(body));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseForS(const nlohmann::json &json) {
  const auto sql_query = json[K_QUERY][K_PLPGSQL_EXPR][K_QUERY].get<std::string>();
  auto parse_result = PostgresParser::BuildParseTree(sql_query);
  if (parse_result == nullptr) {
    return nullptr;
  }
  auto body_stmt = ParseBlock(json[K_BODY]);
  auto variable_array = json[K_ROW][K_PLPGSQL_ROW][K_FIELDS];
  std::vector<std::string> variables{};
  variables.reserve(variable_array.size());
  std::transform(variable_array.cbegin(), variable_array.cend(), std::back_inserter(variables),
                 [](const nlohmann::json &var) { return var[K_NAME].get<std::string>(); });

  if (!AllVariablesDeclared(variables)) {
    throw PARSER_EXCEPTION("PLpgSQL parser : variable was not declared");
  }

  return std::make_unique<execution::ast::udf::ForSStmtAST>(std::move(variables), std::move(parse_result),
                                                            std::move(body_stmt));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseExecSQL(const nlohmann::json &json) {
  // The query text
  const auto sql = json[K_SQLSTMT][K_PLPGSQL_EXPR][K_QUERY].get<std::string>();
  // The variable(s) to which results are bound
  const auto variable_array = json[K_ROW][K_PLPGSQL_ROW][K_FIELDS];
  std::vector<std::string> variables{};
  variables.reserve(variable_array.size());
  std::transform(variable_array.cbegin(), variable_array.cend(), std::back_inserter(variables),
                 [](const nlohmann::json &var) -> std::string { return var[K_NAME].get<std::string>(); });

  return ParseExecSQL(sql, std::move(variables));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseExecSQL(const std::string &sql,
                                                                          std::vector<std::string> &&variables) {
  auto parse_result = StripEnclosingQuery(PostgresParser::BuildParseTree(sql));
  if (!parse_result) {
    throw PARSER_EXCEPTION(fmt::format("PL/pgSQL parser : failed to parse query '{}'", sql));
  }

  // Ensure all variables to which results are bound are declared
  if (!AllVariablesDeclared(variables)) {
    throw PARSER_EXCEPTION("PL/pgSQL parser : variable was not declared");
  }

  /**
   * Two possibilities for binding of results:
   * - Exactly one RECORD variable
   * - One or more non-RECORD variables
   */

  if (ContainsRecordType(variables)) {
    if (variables.size() > 1) {
      throw PARSER_EXCEPTION("Binding of query results is ambiguous");
    }
    // There is only a single result variable and it is a RECORD;
    // derive the structure of the RECORD from the SELECT columns
    const auto &name = variables.front();
    udf_ast_context_->SetRecordType(name, ResolveRecordType(parse_result.get()));
  }

  return std::make_unique<execution::ast::udf::SQLStmtAST>(std::move(parse_result), std::move(variables));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseDynamicSQL(const nlohmann::json &json) {
  auto sql_expr = ParseExprFromSQLString(json[K_QUERY][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
  auto var_name = json[K_ROW][K_PLPGSQL_ROW][K_FIELDS][0][K_NAME].get<std::string>();
  return std::make_unique<execution::ast::udf::DynamicSQLStmtAST>(std::move(sql_expr), std::move(var_name));
}

std::unique_ptr<execution::ast::udf::ExprAST> PLpgSQLParser::ParseExprFromSQLString(const std::string &sql) {
  auto expr = TryParseExprFromSQLString(sql);
  if (!expr.has_value()) {
    throw PARSER_EXCEPTION("PL/pgSQL parser : failed to parse SQL query");
  }
  return std::move(*expr);
}

std::optional<std::unique_ptr<execution::ast::udf::ExprAST>> PLpgSQLParser::TryParseExprFromSQLString(
    const std::string &sql) noexcept {
  auto statements = PostgresParser::BuildParseTree(sql);
  if (!statements) {
    return std::nullopt;
  }

  if (statements->GetStatements().size() != 1) {
    return std::nullopt;
  }
  return TryParseExprFromSQLStatement(statements->GetStatement(0));
}

std::unique_ptr<execution::ast::udf::ExprAST> PLpgSQLParser::ParseExprFromSQLStatement(
    common::ManagedPointer<SQLStatement> statement) {
  auto expr = TryParseExprFromSQLStatement(statement);
  if (!expr.has_value()) {
    throw PARSER_EXCEPTION("PL/pgSQL parser : failed to parse SQL statement");
  }
  return std::move(*expr);
}

std::optional<std::unique_ptr<execution::ast::udf::ExprAST>> PLpgSQLParser::TryParseExprFromSQLStatement(
    common::ManagedPointer<SQLStatement> statement) noexcept {
  if (statement->GetType() != parser::StatementType::SELECT) {
    return std::nullopt;
  }

  auto select = statement.CastManagedPointerTo<parser::SelectStatement>();
  if (select->GetSelectTable() != nullptr || select->GetSelectColumns().size() != 1) {
    return std::nullopt;
  }
  return TryParseExprFromAbstract(select->GetSelectColumns().at(0));
}

std::unique_ptr<execution::ast::udf::ExprAST> PLpgSQLParser::ParseExprFromAbstract(
    common::ManagedPointer<parser::AbstractExpression> expr) {
  auto result = TryParseExprFromAbstract(expr);
  if (!result.has_value()) {
    throw PARSER_EXCEPTION(fmt::format("PL/pgSQL parser : expression type '{}' not supported",
                                       parser::ExpressionTypeToShortString(expr->GetExpressionType())));
  }
  return std::move(*result);
}

std::optional<std::unique_ptr<execution::ast::udf::ExprAST>> PLpgSQLParser::TryParseExprFromAbstract(
    common::ManagedPointer<parser::AbstractExpression> expr) noexcept {
  if ((parser::ExpressionUtil::IsOperatorExpression(expr->GetExpressionType()) && expr->GetChildrenSize() == 2) ||
      (parser::ExpressionUtil::IsComparisonExpression(expr->GetExpressionType()))) {
    auto lhs = TryParseExprFromAbstract(expr->GetChild(0));
    auto rhs = TryParseExprFromAbstract(expr->GetChild(1));
    if (!lhs.has_value() || !rhs.has_value()) {
      return std::nullopt;
    }
    return std::make_optional(std::make_unique<execution::ast::udf::BinaryExprAST>(expr->GetExpressionType(),
                                                                                   std::move(*lhs), std::move(*rhs)));
  }

  // TODO(Kyle): I am not a fan of non-exhaustive switch statements;
  // is there a way that we can refactor this logic to make it better?

  switch (expr->GetExpressionType()) {
    case parser::ExpressionType::COLUMN_VALUE: {
      auto cve = expr.CastManagedPointerTo<parser::ColumnValueExpression>();
      if (cve->GetTableName().empty()) {
        return std::make_optional(std::make_unique<execution::ast::udf::VariableExprAST>(cve->GetColumnName()));
      }
      auto vexpr = std::make_unique<execution::ast::udf::VariableExprAST>(cve->GetTableName());
      return std::make_optional(
          std::make_unique<execution::ast::udf::MemberExprAST>(std::move(vexpr), cve->GetColumnName()));
    }
    case parser::ExpressionType::FUNCTION: {
      auto func_expr = expr.CastManagedPointerTo<parser::FunctionExpression>();
      std::vector<std::unique_ptr<execution::ast::udf::ExprAST>> args{};
      auto num_args = func_expr->GetChildrenSize();
      for (std::size_t idx = 0; idx < num_args; ++idx) {
        auto arg = TryParseExprFromAbstract(func_expr->GetChild(idx));
        if (!arg.has_value()) {
          return std::nullopt;
        }
        args.push_back(std::move(*arg));
      }
      return std::make_optional(
          std::make_unique<execution::ast::udf::CallExprAST>(func_expr->GetFuncName(), std::move(args)));
    }
    case parser::ExpressionType::VALUE_CONSTANT:
      return std::make_optional(std::make_unique<execution::ast::udf::ValueExprAST>(expr->Copy()));
    case parser::ExpressionType::OPERATOR_IS_NOT_NULL: {
      auto target = TryParseExprFromAbstract(expr->GetChild(0));
      if (!target.has_value()) {
        return std::nullopt;
      }
      return std::make_optional(std::make_unique<execution::ast::udf::IsNullExprAST>(false, std::move(*target)));
    }
    case parser::ExpressionType::OPERATOR_IS_NULL: {
      auto target = TryParseExprFromAbstract(expr->GetChild(0));
      if (!target.has_value()) {
        return std::nullopt;
      }
      return std::make_optional(std::make_unique<execution::ast::udf::IsNullExprAST>(true, std::move(*target)));
    }
    case parser::ExpressionType::ROW_SUBQUERY: {
      // We can handle subqeries, but only in the event
      // that they are shallow "wrappers" around simple queries
      auto subquery_expr = expr.CastManagedPointerTo<parser::SubqueryExpression>();
      return TryParseExprFromSQLStatement(subquery_expr->GetSubselect().CastManagedPointerTo<parser::SQLStatement>());
    }
    default:
      return std::nullopt;
  }
}

bool PLpgSQLParser::AllVariablesDeclared(const std::vector<std::string> &names) const {
  return std::all_of(names.cbegin(), names.cend(),
                     [this](const std::string &name) -> bool { return udf_ast_context_->HasVariable(name); });
}

bool PLpgSQLParser::ContainsRecordType(const std::vector<std::string> &names) const {
  return std::any_of(names.cbegin(), names.cend(), [this](const std::string &name) -> bool {
    return udf_ast_context_->GetVariableType(name) == type::TypeId::INVALID;
  });
}

std::vector<std::pair<std::string, type::TypeId>> PLpgSQLParser::ResolveRecordType(const ParseResult *parse_result) {
  std::vector<std::pair<std::string, type::TypeId>> fields{};
  const auto &select_columns =
      parse_result->GetStatement(0).CastManagedPointerTo<const parser::SelectStatement>()->GetSelectColumns();
  fields.reserve(select_columns.size());
  std::transform(select_columns.cbegin(), select_columns.cend(), std::back_inserter(fields),
                 [](const common::ManagedPointer<AbstractExpression> &column) {
                   return std::make_pair(column->GetAlias().GetName(), column->GetReturnValueType());
                 });
  return fields;
}

StatementType PLpgSQLParser::GetStatementType(const std::string &type) {
  StatementType stmt_type;
  if (type == K_PLPGSQL_STMT_RETURN) {
    stmt_type = StatementType::RETURN;
  } else if (type == K_PLPGSQL_STMT_IF) {
    stmt_type = StatementType::IF;
  } else if (type == K_PLPGSQL_STMT_ASSIGN) {
    stmt_type = StatementType::ASSIGN;
  } else if (type == K_PLPGSQL_STMT_WHILE) {
    stmt_type = StatementType::WHILE;
  } else if (type == K_PLPGSQL_STMT_FORI) {
    stmt_type = StatementType::FORI;
  } else if (type == K_PLPGSQL_STMT_FORS) {
    stmt_type = StatementType::FORS;
  } else if (type == K_PLGPSQL_STMT_EXECSQL) {
    stmt_type = StatementType::EXECSQL;
  } else if (type == K_PLPGSQL_STMT_DYNEXECUTE) {
    stmt_type = StatementType::DYNEXECUTE;
  } else {
    stmt_type = StatementType::UNKNOWN;
  }
  return stmt_type;
}

// Static
std::unique_ptr<parser::ParseResult> PLpgSQLParser::StripEnclosingQuery(std::unique_ptr<ParseResult> &&input) {
  NOISEPAGE_ASSERT(input->GetStatements().size() == 1, "Must have a single SQL statement");

  // If the query does not match the target pattern, return unmodified
  if (!HasEnclosingQuery(input.get())) {
    return std::move(input);
  }

  // The query consists of enclosing SELECT around a
  // subquery that implements the actual logic we want;
  // now we perform some surgery on the ParseResult

  // Grab the SELECT from the subquery expression
  auto statement = input->GetStatement(0);
  auto select = statement.CastManagedPointerTo<parser::SelectStatement>();
  auto subquery = select->GetSelectColumns().at(0).CastManagedPointerTo<parser::SubqueryExpression>();

  // Here, we take ownership of the new top-level statement for the query;
  // the SELECT does not own its own target expressions, however, so we
  // need to ensure that we manually copy these over into the new ParseResult
  // such that their data is still alive after the transformation
  auto subselect = subquery->ReleaseSubselect();

  // Take ownership of the expressions we want; it is important
  // that we actually take ownership of the existing expressions
  // rather than making a copy of the collection because the
  // remainder of the statements in the query hold non-owning
  // pointers to these existing expressions, copies will result
  // in dangling pointers in any number of the query statements
  auto expressions = input->TakeExpressionsOwnership();
  expressions.erase(std::remove_if(expressions.begin(), expressions.end(),
                                   [](const std::unique_ptr<parser::AbstractExpression> &expr) {
                                     return expr->GetExpressionType() == parser::ExpressionType::ROW_SUBQUERY;
                                   }),
                    expressions.end());

  auto output = std::make_unique<parser::ParseResult>();
  output->AddStatement(std::move(subselect));
  for (auto &expression : expressions) {
    output->AddExpression(std::move(expression));
  }

  // The input ParseResult is dropped here, so we need to be sure
  // that we have extracted all of the data that we want out of it

  return output;
}

bool PLpgSQLParser::HasEnclosingQuery(ParseResult *parse_result) {
  NOISEPAGE_ASSERT(parse_result->GetStatements().size() == 1, "Must have a single SQL statement");
  auto statement = parse_result->GetStatement(0);
  if (statement->GetType() != parser::StatementType::SELECT) {
    return false;
  }
  auto select = statement.CastManagedPointerTo<SelectStatement>();
  if (select->GetSelectColumns().size() > 1) {
    return false;
  }
  auto target = select->GetSelectColumns().at(0);
  return (target->GetExpressionType() == parser::ExpressionType::ROW_SUBQUERY);
}

}  // namespace noisepage::parser::udf
