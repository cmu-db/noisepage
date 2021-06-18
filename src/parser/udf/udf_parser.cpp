#include <sstream>

#include "binder/bind_node_visitor.h"
#include "execution/ast/udf/udf_ast_nodes.h"
#include "parser/udf/udf_parser.h"

#include "libpg_query/pg_query.h"
#include "nlohmann/json.hpp"

namespace noisepage {
namespace parser {
namespace udf {

/**
 * @brief The identifiers used as keys in the parse tree.
 */
static constexpr const char kFunctionList[] = "FunctionList";
static constexpr const char kDatums[] = "datums";
static constexpr const char kPLpgSQL_var[] = "PLpgSQL_var";
static constexpr const char kRefname[] = "refname";
static constexpr const char kDatatype[] = "datatype";
static constexpr const char kDefaultVal[] = "default_val";
static constexpr const char kPLpgSQL_type[] = "PLpgSQL_type";
static constexpr const char kTypname[] = "typname";
static constexpr const char kAction[] = "action";
static constexpr const char kPLpgSQL_function[] = "PLpgSQL_function";
static constexpr const char kBody[] = "body";
static constexpr const char kPLpgSQL_stmt_block[] = "PLpgSQL_stmt_block";
static constexpr const char kPLpgSQL_stmt_return[] = "PLpgSQL_stmt_return";
static constexpr const char kPLpgSQL_stmt_if[] = "PLpgSQL_stmt_if";
static constexpr const char kPLpgSQL_stmt_while[] = "PLpgSQL_stmt_while";
static constexpr const char kPLpgSQL_stmt_fors[] = "PLpgSQL_stmt_fors";
static constexpr const char kCond[] = "cond";
static constexpr const char kThenBody[] = "then_body";
static constexpr const char kElseBody[] = "else_body";
static constexpr const char kExpr[] = "expr";
static constexpr const char kQuery[] = "query";
static constexpr const char kPLpgSQL_expr[] = "PLpgSQL_expr";
static constexpr const char kPLpgSQL_stmt_assign[] = "PLpgSQL_stmt_assign";
static constexpr const char kVarno[] = "varno";
static constexpr const char kPLpgSQL_stmt_execsql[] = "PLpgSQL_stmt_execsql";
static constexpr const char kSqlstmt[] = "sqlstmt";
static constexpr const char kRow[] = "row";
static constexpr const char kFields[] = "fields";
static constexpr const char kName[] = "name";
static constexpr const char kPLpgSQL_row[] = "PLpgSQL_row";
static constexpr const char kPLpgSQL_stmt_dynexecute[] = "PLpgSQL_stmt_dynexecute";

std::unique_ptr<execution::ast::udf::FunctionAST> PLpgSQLParser::ParsePLpgSQL(
    std::vector<std::string> &&param_names, std::vector<type::TypeId> &&param_types, const std::string &func_body,
    common::ManagedPointer<execution::ast::udf::UDFASTContext> ast_context) {
  auto result = pg_query_parse_plpgsql(func_body.c_str());
  if (result.error) {
    pg_query_free_plpgsql_parse_result(result);
    throw PARSER_EXCEPTION("PL/pgSQL parsing error");
  }
  // The result is a list, we need to wrap it
  const auto ast_json_str =
      "{ \"" + std::string{kFunctionList} + "\" : " + std::string{result.plpgsql_funcs} + " }";  // NOLINT

  pg_query_free_plpgsql_parse_result(result);

  std::istringstream ss{ast_json_str};
  nlohmann::json ast_json{};
  ss >> ast_json;
  const auto function_list = ast_json[kFunctionList];
  NOISEPAGE_ASSERT(function_list.is_array(), "Function list is not an array");
  if (function_list.size() != 1) {
    throw PARSER_EXCEPTION("Function list has size other than 1");
  }

  std::size_t i{0};
  for (const auto &udf_name : param_names) {
    udf_ast_context_->SetVariableType(udf_name, param_types[i++]);
  }
  const auto function = function_list[0][kPLpgSQL_function];
  auto function_ast = std::make_unique<execution::ast::udf::FunctionAST>(
      ParseFunction(function), std::move(param_names), std::move(param_types));
  return function_ast;
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseFunction(const nlohmann::json &block) {
  const auto decl_list = block[kDatums];
  const auto function_body = block[kAction][kPLpgSQL_stmt_block][kBody];

  std::vector<std::unique_ptr<execution::ast::udf::StmtAST>> stmts{};

  NOISEPAGE_ASSERT(decl_list.is_array(), "Declaration list is not an array");
  for (std::size_t i = 1UL; i < decl_list.size(); i++) {
    stmts.push_back(ParseDecl(decl_list[i]));
  }

  stmts.push_back(ParseBlock(function_body));
  return std::make_unique<execution::ast::udf::SeqStmtAST>(std::move(stmts));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseBlock(const nlohmann::json &block) {
  // TODO(boweic): Support statements size other than 1
  NOISEPAGE_ASSERT(block.is_array(), "Block isn't array");
  if (block.size() == 0) {
    throw PARSER_EXCEPTION("PL/pgSQL parser : Empty block is not supported");
  }

  std::vector<std::unique_ptr<execution::ast::udf::StmtAST>> stmts{};

  for (uint32_t i = 0; i < block.size(); i++) {
    const auto stmt = block[i];
    const auto stmt_names = stmt.items().begin();

    if (stmt_names.key() == kPLpgSQL_stmt_return) {
      auto expr = ParseExprSQL(stmt[kPLpgSQL_stmt_return][kExpr][kPLpgSQL_expr][kQuery].get<std::string>());
      // TODO(Kyle): Handle return stmt w/o expression
      stmts.push_back(std::make_unique<execution::ast::udf::RetStmtAST>(std::move(expr)));
    } else if (stmt_names.key() == kPLpgSQL_stmt_if) {
      stmts.push_back(ParseIf(stmt[kPLpgSQL_stmt_if]));
    } else if (stmt_names.key() == kPLpgSQL_stmt_assign) {
      // TODO(Kyle): Need to fix Assignment expression / statement
      const auto &var_name =
          udf_ast_context_->GetLocalVariableAtIndex(stmt[kPLpgSQL_stmt_assign][kVarno].get<std::size_t>());
      auto lhs = std::make_unique<execution::ast::udf::VariableExprAST>(var_name);
      auto rhs = ParseExprSQL(stmt[kPLpgSQL_stmt_assign][kExpr][kPLpgSQL_expr][kQuery].get<std::string>());
      stmts.push_back(std::make_unique<execution::ast::udf::AssignStmtAST>(std::move(lhs), std::move(rhs)));
    } else if (stmt_names.key() == kPLpgSQL_stmt_while) {
      stmts.push_back(ParseWhile(stmt[kPLpgSQL_stmt_while]));
    } else if (stmt_names.key() == kPLpgSQL_stmt_fors) {
      stmts.push_back(ParseFor(stmt[kPLpgSQL_stmt_fors]));
    } else if (stmt_names.key() == kPLpgSQL_stmt_execsql) {
      stmts.push_back(ParseSQL(stmt[kPLpgSQL_stmt_execsql]));
    } else if (stmt_names.key() == kPLpgSQL_stmt_dynexecute) {
      stmts.push_back(ParseDynamicSQL(stmt[kPLpgSQL_stmt_dynexecute]));
    } else {
      throw PARSER_EXCEPTION("Statement type not supported");
    }
  }

  return std::make_unique<execution::ast::udf::SeqStmtAST>(std::move(stmts));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseDecl(const nlohmann::json &decl) {
  const auto &decl_names = decl.items().begin();

  if (decl_names.key() == kPLpgSQL_var) {
    auto var_name = decl[kPLpgSQL_var][kRefname].get<std::string>();
    udf_ast_context_->AddVariable(var_name);
    auto type = decl[kPLpgSQL_var][kDatatype][kPLpgSQL_type][kTypname].get<std::string>();
    std::unique_ptr<execution::ast::udf::ExprAST> initial = nullptr;
    if (decl[kPLpgSQL_var].find(kDefaultVal) != decl[kPLpgSQL_var].end()) {
      initial = ParseExprSQL(decl[kPLpgSQL_var][kDefaultVal][kPLpgSQL_expr][kQuery].get<std::string>());
    }

    type::TypeId temp_type{};
    if (udf_ast_context_->GetVariableType(var_name, &temp_type)) {
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, temp_type, std::move(initial));
    }

    if ((type.find("integer") != std::string::npos) || type.find("INTEGER") != std::string::npos) {
      udf_ast_context_->SetVariableType(var_name, type::TypeId::INTEGER);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::INTEGER, std::move(initial));
    } else if (type == "double" || type.rfind("numeric") == 0) {
      udf_ast_context_->SetVariableType(var_name, type::TypeId::DECIMAL);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::DECIMAL, std::move(initial));
    } else if (type == "varchar") {
      udf_ast_context_->SetVariableType(var_name, type::TypeId::VARCHAR);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::VARCHAR, std::move(initial));
    } else if (type.find("date") != std::string::npos) {
      udf_ast_context_->SetVariableType(var_name, type::TypeId::DATE);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::DATE, std::move(initial));
    } else if (type == "record") {
      udf_ast_context_->SetVariableType(var_name, type::TypeId::INVALID);
      return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::INVALID, std::move(initial));
    } else {
      NOISEPAGE_ASSERT(false, "Unsupported Type");
    }
  } else if (decl_names.key() == kPLpgSQL_row) {
    auto var_name = decl[kPLpgSQL_row][kRefname].get<std::string>();
    NOISEPAGE_ASSERT(var_name == "*internal*", "Unexpected refname");
    // TODO(Kyle): Support row types later
    udf_ast_context_->SetVariableType(var_name, type::TypeId::INVALID);
    return std::make_unique<execution::ast::udf::DeclStmtAST>(var_name, type::TypeId::INVALID, nullptr);
  }
  // TODO(Kyle): Need to handle other types like row, table etc;
  throw PARSER_EXCEPTION("Declaration type not supported");
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseIf(const nlohmann::json &branch) {
  auto cond_expr = ParseExprSQL(branch[kCond][kPLpgSQL_expr][kQuery].get<std::string>());
  auto then_stmt = ParseBlock(branch[kThenBody]);
  std::unique_ptr<execution::ast::udf::StmtAST> else_stmt = nullptr;
  if (branch.find(kElseBody) != branch.end()) {
    else_stmt = ParseBlock(branch[kElseBody]);
  }
  return std::make_unique<execution::ast::udf::IfStmtAST>(std::move(cond_expr), std::move(then_stmt),
                                                          std::move(else_stmt));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseWhile(const nlohmann::json &loop) {
  auto cond_expr = ParseExprSQL(loop[kCond][kPLpgSQL_expr][kQuery].get<std::string>());
  auto body_stmt = ParseBlock(loop[kBody]);
  return std::make_unique<execution::ast::udf::WhileStmtAST>(std::move(cond_expr), std::move(body_stmt));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseFor(const nlohmann::json &loop) {
  auto sql_query = loop[kQuery][kPLpgSQL_expr][kQuery].get<std::string>();
  auto parse_result = PostgresParser::BuildParseTree(sql_query.c_str());
  if (parse_result == nullptr) {
    return nullptr;
  }
  auto body_stmt = ParseBlock(loop[kBody]);
  auto var_array = loop[kRow][kPLpgSQL_row][kFields];
  std::vector<std::string> var_vec;
  for (auto var : var_array) {
    var_vec.push_back(var[kName].get<std::string>());
  }
  return std::make_unique<execution::ast::udf::ForStmtAST>(std::move(var_vec), std::move(parse_result),
                                                           std::move(body_stmt));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseSQL(const nlohmann::json &sql_stmt) {
  auto sql_query = sql_stmt[kSqlstmt][kPLpgSQL_expr][kQuery].get<std::string>();
  auto var_name = sql_stmt[kRow][kPLpgSQL_row][kFields][0][kName].get<std::string>();

  auto parse_result = PostgresParser::BuildParseTree(sql_query.c_str());
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
  type::TypeId type{};
  auto ret = udf_ast_context_->GetVariableType(var_name, &type);
  if (!ret) {
    throw PARSER_EXCEPTION("PL/pgSQL parser: variable was not declared");
  }

  if (type == type::TypeId::INVALID) {
    std::vector<std::pair<std::string, type::TypeId>> elems{};
    const auto &select_columns =
        parse_result->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>()->GetSelectColumns();
    elems.reserve(select_columns.size());
    for (const auto &col : select_columns) {
      elems.emplace_back(col->GetAlias().GetName(), col->GetReturnValueType());
    }
    udf_ast_context_->SetRecordType(var_name, std::move(elems));
  }

  return std::make_unique<execution::ast::udf::SQLStmtAST>(std::move(parse_result), std::move(var_name),
                                                           std::move(query_params));
}

std::unique_ptr<execution::ast::udf::StmtAST> PLpgSQLParser::ParseDynamicSQL(const nlohmann::json &sql_stmt) {
  auto sql_expr = ParseExprSQL(sql_stmt[kQuery][kPLpgSQL_expr][kQuery].get<std::string>());
  auto var_name = sql_stmt[kRow][kPLpgSQL_row][kFields][0][kName].get<std::string>();
  return std::make_unique<execution::ast::udf::DynamicSQLStmtAST>(std::move(sql_expr), std::move(var_name));
}

std::unique_ptr<execution::ast::udf::ExprAST> PLpgSQLParser::ParseExprSQL(const std::string &expr_sql_str) {
  auto stmt_list = PostgresParser::BuildParseTree(expr_sql_str);
  if (stmt_list == nullptr) {
    return nullptr;
  }
  NOISEPAGE_ASSERT(stmt_list->GetStatements().size() == 1, "Bad number of statements");
  auto stmt = stmt_list->GetStatement(0);
  NOISEPAGE_ASSERT(stmt->GetType() == parser::StatementType::SELECT, "Unsupported statement type");
  NOISEPAGE_ASSERT(stmt.CastManagedPointerTo<parser::SelectStatement>()->GetSelectTable() == nullptr,
                   "Unsupported SQL Expr in UDF");
  auto &select_list = stmt.CastManagedPointerTo<parser::SelectStatement>()->GetSelectColumns();
  NOISEPAGE_ASSERT(select_list.size() == 1, "Unsupported number of select columns in udf");
  return PLpgSQLParser::ParseExpr(select_list[0]);
}

std::unique_ptr<execution::ast::udf::ExprAST> PLpgSQLParser::ParseExpr(
    common::ManagedPointer<parser::AbstractExpression> expr) {
  if (expr->GetExpressionType() == parser::ExpressionType::COLUMN_VALUE) {
    auto cve = expr.CastManagedPointerTo<parser::ColumnValueExpression>();
    if (cve->GetTableName().empty()) {
      return std::make_unique<execution::ast::udf::VariableExprAST>(cve->GetColumnName());
    } else {
      auto vexpr = std::make_unique<execution::ast::udf::VariableExprAST>(cve->GetTableName());
      return std::make_unique<execution::ast::udf::MemberExprAST>(std::move(vexpr), cve->GetColumnName());
    }
  } else if ((parser::ExpressionUtil::IsOperatorExpression(expr->GetExpressionType()) &&
              expr->GetChildrenSize() == 2) ||
             (parser::ExpressionUtil::IsComparisonExpression(expr->GetExpressionType()))) {
    return std::make_unique<execution::ast::udf::BinaryExprAST>(expr->GetExpressionType(), ParseExpr(expr->GetChild(0)),
                                                                ParseExpr(expr->GetChild(1)));
  } else if (expr->GetExpressionType() == parser::ExpressionType::FUNCTION) {
    auto func_expr = expr.CastManagedPointerTo<parser::FunctionExpression>();
    std::vector<std::unique_ptr<execution::ast::udf::ExprAST>> args{};
    auto num_args = func_expr->GetChildrenSize();
    for (size_t idx = 0; idx < num_args; ++idx) {
      args.push_back(ParseExpr(func_expr->GetChild(idx)));
    }
    return std::make_unique<execution::ast::udf::CallExprAST>(func_expr->GetFuncName(), std::move(args));
  } else if (expr->GetExpressionType() == parser::ExpressionType::VALUE_CONSTANT) {
    return std::make_unique<execution::ast::udf::ValueExprAST>(expr->Copy());
  } else if (expr->GetExpressionType() == parser::ExpressionType::OPERATOR_IS_NOT_NULL) {
    return std::make_unique<execution::ast::udf::IsNullExprAST>(false, ParseExpr(expr->GetChild(0)));
  } else if (expr->GetExpressionType() == parser::ExpressionType::OPERATOR_IS_NULL) {
    return std::make_unique<execution::ast::udf::IsNullExprAST>(true, ParseExpr(expr->GetChild(0)));
  }
  throw PARSER_EXCEPTION("PL/pgSQL parser : Expression type not supported");
}

}  // namespace udf
}  // namespace parser
}  // namespace noisepage
