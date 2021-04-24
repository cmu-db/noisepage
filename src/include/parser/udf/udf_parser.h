#pragma once

#include <memory>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "execution/ast/udf/udf_ast_context.h"
#include "execution/ast/udf/udf_ast_nodes.h"

#include "parser/expression_util.h"
#include "parser/postgresparser.h"

namespace noisepage {

// Forward declaration
namespace execution::ast::udf {
class FunctionAST;
}

namespace parser {
namespace udf {
/**
 * Namespace alias to make below more manageable.
 */
namespace udfexec = execution::ast::udf;

class PLpgSQLParser {
 public:
  PLpgSQLParser(common::ManagedPointer<udfexec::UDFASTContext> udf_ast_context,
                const common::ManagedPointer<catalog::CatalogAccessor> accessor, catalog::db_oid_t db_oid)
      : udf_ast_context_(udf_ast_context), accessor_(accessor), db_oid_(db_oid) {}
  std::unique_ptr<udfexec::FunctionAST> ParsePLpgSQL(std::vector<std::string> &&param_names,
                                                     std::vector<type::TypeId> &&param_types,
                                                     const std::string &func_body,
                                                     common::ManagedPointer<udfexec::UDFASTContext> ast_context);

 private:
  std::unique_ptr<udfexec::StmtAST> ParseBlock(const nlohmann::json &block);
  std::unique_ptr<udfexec::StmtAST> ParseFunction(const nlohmann::json &block);
  std::unique_ptr<udfexec::StmtAST> ParseDecl(const nlohmann::json &decl);
  std::unique_ptr<udfexec::StmtAST> ParseIf(const nlohmann::json &branch);
  std::unique_ptr<udfexec::StmtAST> ParseWhile(const nlohmann::json &loop);
  std::unique_ptr<udfexec::StmtAST> ParseFor(const nlohmann::json &loop);
  std::unique_ptr<udfexec::StmtAST> ParseSQL(const nlohmann::json &sql_stmt);
  std::unique_ptr<udfexec::StmtAST> ParseDynamicSQL(const nlohmann::json &sql_stmt);
  // Feed the expression (as a sql string) to our parser then transform the
  // noisepage expression into ast node
  std::unique_ptr<udfexec::ExprAST> ParseExprSQL(const std::string expr_sql_str);
  std::unique_ptr<udfexec::ExprAST> ParseExpr(common::ManagedPointer<parser::AbstractExpression>);

  common::ManagedPointer<udfexec::UDFASTContext> udf_ast_context_;
  const common::ManagedPointer<catalog::CatalogAccessor> accessor_;
  catalog::db_oid_t db_oid_;
  //  common::ManagedPointer<parser::PostgresParser> sql_parser_;
  std::unordered_map<std::string, type::TypeId> symbol_table_;
};

}  // namespace udf
}  // namespace parser
}  // namespace noisepage
