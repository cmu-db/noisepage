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
namespace compiler {
namespace udf {

// TODO(Kyle): Is distinguishing the standard codegen
// namespace stuff from the UDF stuff here going to be
// an issue (i.e. disambiguation)?

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
class DynamicSQLStmtAST;
class ForStmtAST;

class UDFCodegen : ASTNodeVisitor {
 public:
  UDFCodegen(catalog::CatalogAccessor *accessor, FunctionBuilder *fb, parser::udf::UDFASTContext *udf_ast_context,
             CodeGen *codegen, catalog::db_oid_t db_oid);
  ~UDFCodegen(){};

  catalog::type_oid_t GetCatalogTypeOidFromSQLType(execution::ast::BuiltinType::Kind type);

  void GenerateUDF(AbstractAST *);
  void Visit(AbstractAST *) override;
  void Visit(FunctionAST *) override;
  void Visit(StmtAST *) override;
  void Visit(ExprAST *) override;
  void Visit(ValueExprAST *) override;
  void Visit(VariableExprAST *) override;
  void Visit(BinaryExprAST *) override;
  void Visit(CallExprAST *) override;
  void Visit(IsNullExprAST *) override;
  void Visit(SeqStmtAST *) override;
  void Visit(DeclStmtAST *) override;
  void Visit(IfStmtAST *) override;
  void Visit(WhileStmtAST *) override;
  void Visit(RetStmtAST *) override;
  void Visit(AssignStmtAST *) override;
  void Visit(SQLStmtAST *) override;
  void Visit(DynamicSQLStmtAST *) override;
  void Visit(ForStmtAST *) override;
  void Visit(MemberExprAST *) override;

  execution::ast::File *Finish() {
    auto fn = fb_->Finish();
    ////  util::RegionVector<ast::Decl *> decls_reg_vec{decls->begin(), decls->end(), codegen.Region()};
    execution::util::RegionVector<execution::ast::Decl *> decls({fn}, codegen_->GetAstContext()->GetRegion());
    //    for(auto decl : aux_decls_){
    //      decls.push_back(decl);
    //    }
    decls.insert(decls.begin(), aux_decls_.begin(), aux_decls_.end());
    auto file = codegen_->GetAstContext()->GetNodeFactory()->NewFile({0, 0}, std::move(decls));
    return file;
  }

  static const char *GetReturnParamString();

 private:
  catalog::CatalogAccessor *accessor_;
  FunctionBuilder *fb_;
  UDFASTContext *udf_ast_context_;
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
