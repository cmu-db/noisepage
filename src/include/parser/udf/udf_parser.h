#pragma once

#include <memory>
#include <vector>

#include "ast_nodes.h"
#include "catalog/catalog_accessor.h"

#include "parser/expression_util.h"
#include "parser/postgresparser.h"
#include "parser/udf/udf_ast_context.h"

// TODO(Kyle): Do we want to place UDF parsing in its own namespace?
namespace noisepage {
namespace parser {
namespace udf {

class FunctionAST;
class PLpgSQLParser {
 public:
  PLpgSQLParser(common::ManagedPointer<UDFASTContext> udf_ast_context,
                const common::ManagedPointer<catalog::CatalogAccessor> accessor, catalog::db_oid_t db_oid)
      : udf_ast_context_(udf_ast_context), accessor_(accessor), db_oid_(db_oid) {}
  std::unique_ptr<FunctionAST> ParsePLpgSQL(std::vector<std::string> &&param_names,
                                            std::vector<type::TypeId> &&param_types, const std::string &func_body,
                                            common::ManagedPointer<UDFASTContext> ast_context);

 private:
  std::unique_ptr<StmtAST> ParseBlock(const nlohmann::json &block);
  std::unique_ptr<StmtAST> ParseFunction(const nlohmann::json &block);
  std::unique_ptr<StmtAST> ParseDecl(const nlohmann::json &decl);
  std::unique_ptr<StmtAST> ParseIf(const nlohmann::json &branch);
  std::unique_ptr<StmtAST> ParseWhile(const nlohmann::json &loop);
  std::unique_ptr<StmtAST> ParseFor(const nlohmann::json &loop);
  std::unique_ptr<StmtAST> ParseSQL(const nlohmann::json &sql_stmt);
  std::unique_ptr<StmtAST> ParseDynamicSQL(const nlohmann::json &sql_stmt);
  // Feed the expression (as a sql string) to our parser then transform the
  // noisepage expression into ast node
  std::unique_ptr<ExprAST> ParseExprSQL(const std::string expr_sql_str);
  std::unique_ptr<ExprAST> ParseExpr(common::ManagedPointer<parser::AbstractExpression>);

  common::ManagedPointer<UDFASTContext> udf_ast_context_;
  const common::ManagedPointer<catalog::CatalogAccessor> accessor_;
  catalog::db_oid_t db_oid_;
  //  common::ManagedPointer<parser::PostgresParser> sql_parser_;
  std::unordered_map<std::string, type::TypeId> symbol_table_;
};

}  // namespace udf
}  // namespace parser
}  // namespace noisepage
