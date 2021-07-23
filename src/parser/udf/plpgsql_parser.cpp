#include <sstream>

#include "binder/bind_node_visitor.h"
#include "execution/ast/udf/udf_ast_nodes.h"
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
  auto result = pg_query_parse_plpgsql(func_body.c_str());
  if (result.error != nullptr) {
    pg_query_free_plpgsql_parse_result(result);
    throw PARSER_EXCEPTION("PL/pgSQL parsing error");
  }
  // The result is a list, we need to wrap it
  const auto ast_json_str = fmt::format("{{ \"{}\" : {} }}", K_FUNCTION_LIST, result.plpgsql_funcs);
  pg_query_free_plpgsql_parse_result(result);

  std::istringstream ss{ast_json_str};
  nlohmann::json ast_json{};
  ss >> ast_json;
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
  const auto decl_list = json[K_DATUMS];
  NOISEPAGE_ASSERT(decl_list.is_array(), "Declaration list is not an array");

  const auto function_body = json[K_ACTION][K_PLPGSQL_STMT_BLOCK][K_BODY];

  std::vector<std::unique_ptr<execution::ast::udf::StmtAST>> stmts{};

  for (std::size_t i = 1UL; i < decl_list.size(); i++) {
    stmts.push_back(ParseDecl(decl_list[i]));
  }

  stmts.push_back(ParseBlock(function_body));
  return std::make_unique<execution::ast::udf::SeqStmtAST>(std::move(stmts));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseBlock(const nlohmann::json &json) {
  // TODO(boweic): Support statements size other than 1
  NOISEPAGE_ASSERT(json.is_array(), "Block isn't array");
  if (json.empty()) {
    throw PARSER_EXCEPTION("PL/pgSQL parser : Empty block is not supported");
  }

  std::vector<std::unique_ptr<execution::ast::udf::StmtAST>> statements{};
  for (const auto &statement : json) {
    std::cout << statement << std::endl;
    const std::string &statement_type = statement.items().begin().key();
    if (statement_type == K_PLPGSQL_STMT_RETURN) {
      // TODO(Kyle): Handle RETURN without expression
      if (statement[K_PLPGSQL_STMT_RETURN].empty()) {
        throw NOT_IMPLEMENTED_EXCEPTION("RETURN without expression not implemented.");
      }
      auto expr =
          ParseExprFromSQL(statement[K_PLPGSQL_STMT_RETURN][K_EXPR][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
      statements.push_back(std::make_unique<execution::ast::udf::RetStmtAST>(std::move(expr)));
    } else if (statement_type == K_PLPGSQL_STMT_IF) {
      statements.push_back(ParseIf(statement[K_PLPGSQL_STMT_IF]));
    } else if (statement_type == K_PLPGSQL_STMT_ASSIGN) {
      // TODO(Kyle): Need to fix Assignment expression / statement
      // NOTE(Kyle): We subtract 1 here because variable numbers from
      // the Postres parser index from 1 rather than 0 (?)
      const auto &var_name =
          udf_ast_context_->GetLocalAtIndex(statement[K_PLPGSQL_STMT_ASSIGN][K_VARNO].get<std::size_t>() - 1);
      auto lhs = std::make_unique<execution::ast::udf::VariableExprAST>(var_name);
      auto rhs = ParseExprFromSQL(statement[K_PLPGSQL_STMT_ASSIGN][K_EXPR][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
      statements.push_back(std::make_unique<execution::ast::udf::AssignStmtAST>(std::move(lhs), std::move(rhs)));
    } else if (statement_type == K_PLPGSQL_STMT_WHILE) {
      statements.push_back(ParseWhile(statement[K_PLPGSQL_STMT_WHILE]));
    } else if (statement_type == K_PLPGSQL_STMT_FORI) {
      statements.push_back(ParseForI(statement[K_PLPGSQL_STMT_FORI]));
    } else if (statement_type == K_PLPGSQL_STMT_FORS) {
      statements.push_back(ParseForS(statement[K_PLPGSQL_STMT_FORS]));
    } else if (statement_type == K_PLGPSQL_STMT_EXECSQL) {
      statements.push_back(ParseSQL(statement[K_PLGPSQL_STMT_EXECSQL]));
    } else if (statement_type == K_PLPGSQL_STMT_DYNEXECUTE) {
      statements.push_back(ParseDynamicSQL(statement[K_PLPGSQL_STMT_DYNEXECUTE]));
    } else {
      throw PARSER_EXCEPTION(fmt::format("Statement type '{}' not supported", statement_type));
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
      initial = ParseExprFromSQL(json[K_PLPGSQL_VAR][K_DEFAULT_VAL][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
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

    throw PARSER_EXCEPTION(fmt::format("Unsupported type '{}' for variable '{}'", type, var_name));
  }

  if (declaration_type == K_PLPGSQL_ROW) {
    const auto var_name = json[K_PLPGSQL_ROW][K_REFNAME].get<std::string>();
    NOISEPAGE_ASSERT(var_name == "*internal*", "Unexpected refname");

    // TODO(Kyle): Support row types later
    udf_ast_context_->SetVariableType(var_name, type::TypeId::INVALID);
    return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::INVALID, nullptr);
  }

  // TODO(Kyle): Need to handle other types like row, table etc;
  throw PARSER_EXCEPTION(fmt::format("Declaration type '{}' not supported", declaration_type));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseIf(const nlohmann::json &json) {
  auto cond_expr = ParseExprFromSQL(json[K_COND][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
  auto then_stmt = ParseBlock(json[K_THEN_BODY]);
  std::unique_ptr<execution::ast::udf::StmtAST> else_stmt =
      json.contains(K_ELSE_BODY) ? ParseBlock(json[K_ELSE_BODY]) : nullptr;
  return std::make_unique<execution::ast::udf::IfStmtAST>(std::move(cond_expr), std::move(then_stmt),
                                                          std::move(else_stmt));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseWhile(const nlohmann::json &json) {
  auto cond_expr = ParseExprFromSQL(json[K_COND][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
  auto body_stmt = ParseBlock(json[K_BODY]);
  return std::make_unique<execution::ast::udf::WhileStmtAST>(std::move(cond_expr), std::move(body_stmt));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseForI(const nlohmann::json &json) {
  const auto name = json[K_VAR][K_PLPGSQL_VAR][K_REFNAME].get<std::string>();
  auto lower = ParseExprFromSQL(json[K_LOWER][K_PLPGSQL_EXPR][K_QUERY]);
  auto upper = ParseExprFromSQL(json[K_UPPER][K_PLPGSQL_EXPR][K_QUERY]);
  auto step = json.contains(K_STEP) ? ParseExprFromSQL(json[K_STEP][K_PLPGSQL_EXPR][K_QUERY])
                                    : ParseExprFromSQL(execution::ast::udf::ForIStmtAST::DEFAULT_STEP_EXPR);
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
  auto var_array = json[K_ROW][K_PLPGSQL_ROW][K_FIELDS];
  std::vector<std::string> variables{};
  variables.reserve(var_array.size());
  std::transform(var_array.cbegin(), var_array.cend(), std::back_inserter(variables),
                 [](const nlohmann::json &var) { return var[K_NAME].get<std::string>(); });
  return std::make_unique<execution::ast::udf::ForSStmtAST>(std::move(variables), std::move(parse_result),
                                                            std::move(body_stmt));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseSQL(const nlohmann::json &json) {
  // The query text
  const auto sql_query = json[K_SQLSTMT][K_PLPGSQL_EXPR][K_QUERY].get<std::string>();
  // The variable name (non-const for later std::move)
  auto var_name = json[K_ROW][K_PLPGSQL_ROW][K_FIELDS][0][K_NAME].get<std::string>();

  auto parse_result = PostgresParser::BuildParseTree(sql_query);
  if (parse_result == nullptr) {
    return nullptr;
  }

  binder::BindNodeVisitor visitor{accessor_, db_oid_};
  std::unordered_map<std::string, std::pair<std::string, size_t>> query_params{};
  try {
    // TODO(Matt): I don't think the binder should need the database name.
    // It's already bound in the ConnectionContext binder::BindNodeVisitor visitor(accessor_, db_oid_);
    query_params = visitor.BindAndGetUDFParams(common::ManagedPointer{parse_result}, udf_ast_context_);
  } catch (BinderException &b) {
    return nullptr;
  }

  // Check to see if a record type can be bound to this
  const auto type = udf_ast_context_->GetVariableType(var_name);
  if (!type.has_value()) {
    throw PARSER_EXCEPTION("PL/pgSQL parser: variable was not declared");
  }

  if (type.value() == type::TypeId::INVALID) {
    std::vector<std::pair<std::string, type::TypeId>> elems{};
    const auto &select_columns =
        parse_result->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>()->GetSelectColumns();
    elems.reserve(select_columns.size());
    std::transform(select_columns.cbegin(), select_columns.cend(), std::back_inserter(elems),
                   [](const common::ManagedPointer<AbstractExpression> &column) {
                     return std::make_pair(column->GetAlias().GetName(), column->GetReturnValueType());
                   });
    udf_ast_context_->SetRecordType(var_name, std::move(elems));
  }

  return std::make_unique<execution::ast::udf::SQLStmtAST>(std::move(parse_result), std::move(var_name),
                                                           std::move(query_params));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseDynamicSQL(const nlohmann::json &json) {
  auto sql_expr = ParseExprFromSQL(json[K_QUERY][K_PLPGSQL_EXPR][K_QUERY].get<std::string>());
  auto var_name = json[K_ROW][K_PLPGSQL_ROW][K_FIELDS][0][K_NAME].get<std::string>();
  return std::make_unique<execution::ast::udf::DynamicSQLStmtAST>(std::move(sql_expr), std::move(var_name));
}

std::unique_ptr<execution::ast::udf::ExprAST> PLpgSQLParser::ParseExprFromSQL(const std::string &sql) {
  auto stmt_list = PostgresParser::BuildParseTree(sql);
  if (stmt_list == nullptr) {
    return nullptr;
  }
  NOISEPAGE_ASSERT(stmt_list->GetStatements().size() == 1, "Bad number of statements");
  auto stmt = stmt_list->GetStatement(0);
  NOISEPAGE_ASSERT(stmt->GetType() == parser::StatementType::SELECT, "Unsupported statement type");
  NOISEPAGE_ASSERT(stmt.CastManagedPointerTo<parser::SelectStatement>()->GetSelectTable() == nullptr,
                   "Unsupported SQL Expr in UDF");
  auto &select_list = stmt.CastManagedPointerTo<parser::SelectStatement>()->GetSelectColumns();
  NOISEPAGE_ASSERT(select_list.size() == 1, "Unsupported number of select columns in UDF");
  return PLpgSQLParser::ParseExprFromAbstract(select_list[0]);
}

std::unique_ptr<execution::ast::udf::ExprAST> PLpgSQLParser::ParseExprFromAbstract(
    common::ManagedPointer<parser::AbstractExpression> expr) {
  if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
    auto cve = expr.CastManagedPointerTo<parser::ColumnValueExpression>();
    if (cve->GetTableName().empty()) {
      return std::make_unique<execution::ast::udf::VariableExprAST>(cve->GetColumnName());
    }
    auto vexpr = std::make_unique<execution::ast::udf::VariableExprAST>(cve->GetTableName());
    return std::make_unique<execution::ast::udf::MemberExprAST>(std::move(vexpr), cve->GetColumnName());
  }

  if ((parser::ExpressionUtil::IsOperatorExpression(expr->GetExpressionType()) && expr->GetChildrenSize() == 2) ||
      (parser::ExpressionUtil::IsComparisonExpression(expr->GetExpressionType()))) {
    return std::make_unique<execution::ast::udf::BinaryExprAST>(
        expr->GetExpressionType(), ParseExprFromAbstract(expr->GetChild(0)), ParseExprFromAbstract(expr->GetChild(1)));
  }

  // TODO(Kyle): I am not a fan of non-exhaustive switch statements;
  // is there a way that we can refactor this logic to make it better?

  switch (expr->GetExpressionType()) {
    case parser::ExpressionType::FUNCTION: {
      auto func_expr = expr.CastManagedPointerTo<parser::FunctionExpression>();
      std::vector<std::unique_ptr<execution::ast::udf::ExprAST>> args{};
      auto num_args = func_expr->GetChildrenSize();
      for (size_t idx = 0; idx < num_args; ++idx) {
        args.push_back(ParseExprFromAbstract(func_expr->GetChild(idx)));
      }
      return std::make_unique<execution::ast::udf::CallExprAST>(func_expr->GetFuncName(), std::move(args));
    }
    case parser::ExpressionType::VALUE_CONSTANT:
      return std::make_unique<execution::ast::udf::ValueExprAST>(expr->Copy());
    case parser::ExpressionType::OPERATOR_IS_NOT_NULL:
      return std::make_unique<execution::ast::udf::IsNullExprAST>(false, ParseExprFromAbstract(expr->GetChild(0)));
    case parser::ExpressionType::OPERATOR_IS_NULL:
      return std::make_unique<execution::ast::udf::IsNullExprAST>(true, ParseExprFromAbstract(expr->GetChild(0)));
    default:
      throw PARSER_EXCEPTION("PL/pgSQL parser : Expression type not supported");
  }
}

}  // namespace noisepage::parser::udf
