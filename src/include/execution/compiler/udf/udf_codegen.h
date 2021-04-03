#pragma once

#include "execution/ast/udf/udf_ast_context.h"
#include "execution/ast/udf/udf_ast_node_visitor.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/functions/function_context.h"

// TODO(Kyle): Documentation.

namespace noisepage::catalog {
class CatalogAccessor;
}

namespace noisepage {
namespace execution {

// Forward declarations
namespace ast {
namespace udf {
class AbstractAST;
class StmtAST;
class ExprAST;
class ValueExprAST;
class VariableExprAST;
class BinaryExprAST;
class CallExprAST;
class MemberExprAST;
class SeqStmtAST;
class DeclStmtAST;
class IfStmtAST;
class WhileStmtAST;
class RetStmtAST;
class AssignStmtAST;
class SQLStmtAST;
class FunctionAST;
class IsNullExprAST;
class DynamicSQLStmtAST;
class ForStmtAST;
}  // namespace udf
}  // namespace ast

namespace compiler {
namespace udf {

// TODO(Kyle): Is distinguishing the standard codegen
// namespace stuff from the UDF stuff here going to be
// an issue (i.e. disambiguation)?

class UDFCodegen : ast::udf::ASTNodeVisitor {
 public:
  UDFCodegen(catalog::CatalogAccessor *accessor, FunctionBuilder *fb, ast::udf::UDFASTContext *udf_ast_context,
             CodeGen *codegen, catalog::db_oid_t db_oid);
  ~UDFCodegen(){};

  catalog::type_oid_t GetCatalogTypeOidFromSQLType(execution::ast::BuiltinType::Kind type);

  void GenerateUDF(ast::udf::AbstractAST *);

  void Visit(ast::udf::AbstractAST *) override;
  void Visit(ast::udf::FunctionAST *) override;
  void Visit(ast::udf::StmtAST *) override;
  void Visit(ast::udf::ExprAST *) override;
  void Visit(ast::udf::ValueExprAST *) override;
  void Visit(ast::udf::VariableExprAST *) override;
  void Visit(ast::udf::BinaryExprAST *) override;
  void Visit(ast::udf::CallExprAST *) override;
  void Visit(ast::udf::IsNullExprAST *) override;
  void Visit(ast::udf::SeqStmtAST *) override;
  void Visit(ast::udf::DeclStmtAST *) override;
  void Visit(ast::udf::IfStmtAST *) override;
  void Visit(ast::udf::WhileStmtAST *) override;
  void Visit(ast::udf::RetStmtAST *) override;
  void Visit(ast::udf::AssignStmtAST *) override;
  void Visit(ast::udf::SQLStmtAST *) override;
  void Visit(ast::udf::DynamicSQLStmtAST *) override;
  void Visit(ast::udf::ForStmtAST *) override;
  void Visit(ast::udf::MemberExprAST *) override;

  execution::ast::File *Finish();

  static const char *GetReturnParamString();

 private:
  catalog::CatalogAccessor *accessor_;
  FunctionBuilder *fb_;
  ast::udf::UDFASTContext *udf_ast_context_;
  CodeGen *codegen_;
  type::TypeId current_type_{type::TypeId::INVALID};
  execution::ast::Expr *dst_;
  std::unordered_map<std::string, execution::ast::Identifier> str_to_ident_;
  execution::util::RegionVector<execution::ast::Decl *> aux_decls_;
  catalog::db_oid_t db_oid_;
  bool needs_exec_ctx_{false};
};

}  // namespace udf
}  // namespace compiler
}  // namespace execution
}  // namespace noisepage
