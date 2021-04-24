#pragma once

#include "execution/ast/udf/udf_ast_context.h"
#include "execution/ast/udf/udf_ast_node_visitor.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/function_builder.h"
#include "execution/functions/function_context.h"

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

class UDFCodegen : ast::udf::ASTNodeVisitor {
 public:
  /**
   * Construct a new UDFCodegen instance.
   * @param accessor The catalog accessor used in code generation
   * @param fb The function builder instance used for the UDF
   * @param udf_ast_context The AST context for the UDF
   * @param codegen The codegen instance
   * @param db_oid The OID for the relevant database
   */
  UDFCodegen(catalog::CatalogAccessor *accessor, FunctionBuilder *fb, ast::udf::UDFASTContext *udf_ast_context,
             CodeGen *codegen, catalog::db_oid_t db_oid);

  ~UDFCodegen() = default;

  /**
   * Generate a UDF from the given abstract syntax tree.
   * @param ast The AST from which to generate the UDF
   */
  void GenerateUDF(ast::udf::AbstractAST *ast);

  /**
   * Visit an AbstractAST node.
   */
  void Visit(ast::udf::AbstractAST *) override;

  /**
   * Visit a FunctionAST node.
   */
  void Visit(ast::udf::FunctionAST *) override;

  /**
   * Visit a StmtAST node.
   */
  void Visit(ast::udf::StmtAST *) override;

  /**
   * Visit an ExprAST node.
   */
  void Visit(ast::udf::ExprAST *) override;

  /**
   * Visit a ValueExprAST node.
   */
  void Visit(ast::udf::ValueExprAST *) override;

  /**
   * Visit a VariableExprAST node.
   */
  void Visit(ast::udf::VariableExprAST *) override;

  /**
   * Visit a BinaryExprAST node.
   */
  void Visit(ast::udf::BinaryExprAST *) override;

  /**
   * Visit a CallExprAST node.
   */
  void Visit(ast::udf::CallExprAST *) override;

  /**
   * Visit an IsNullExprAST node.
   */
  void Visit(ast::udf::IsNullExprAST *) override;

  /**
   * Visit a SeqStmtAST node.
   */
  void Visit(ast::udf::SeqStmtAST *) override;

  /**
   * Visit a DeclStmtNode node.
   */
  void Visit(ast::udf::DeclStmtAST *) override;

  /**
   * Visit a IfStmtAST node.
   */
  void Visit(ast::udf::IfStmtAST *) override;

  /**
   * Visit a WhileStmtAST node.
   */
  void Visit(ast::udf::WhileStmtAST *) override;

  /**
   * Visit a RetStmtAST node.
   */
  void Visit(ast::udf::RetStmtAST *) override;

  /**
   * Visit an AssignStmtAST node.
   */
  void Visit(ast::udf::AssignStmtAST *) override;

  /**
   * Visit a SQLStmtAST node.
   */
  void Visit(ast::udf::SQLStmtAST *) override;

  /**
   * Visit a DynamicSQLStmtAST node.
   */
  void Visit(ast::udf::DynamicSQLStmtAST *) override;

  /**
   * Visit a ForStmtAST node.
   */
  void Visit(ast::udf::ForStmtAST *) override;

  /**
   * Visit a MemberExprAST node.
   */
  void Visit(ast::udf::MemberExprAST *) override;

  /**
   * Complete UDF code generation.
   * @return The result of code generation as a file
   */
  execution::ast::File *Finish();

  /**
   * Return the string that represents the return value.
   * @return The string that represents the return value
   */
  static const char *GetReturnParamString();

 private:
  /**
   * Translate a SQL type to its corresponding catalog type.
   * @param type The SQL type of interest
   * @return The corresponding catalog type
   */
  catalog::type_oid_t GetCatalogTypeOidFromSQLType(execution::ast::BuiltinType::Kind type);

  // The catalog access used during code generation
  catalog::CatalogAccessor *accessor_;
  // The function builder used during code generation
  FunctionBuilder *fb_;
  // The AST context for the UDF
  ast::udf::UDFASTContext *udf_ast_context_;
  // The code generation instance
  CodeGen *codegen_;
  // The OID of the relevant database
  catalog::db_oid_t db_oid_;
  // Auxiliary declarations
  execution::util::RegionVector<execution::ast::Decl *> aux_decls_;

  // Flag indicating whether this UDF requires an execution context
  bool needs_exec_ctx_;

  // The current type during code generation
  type::TypeId current_type_{type::TypeId::INVALID};
  // The destination expression
  execution::ast::Expr *dst_;
  // Map from human-readable string identifier to internal identifier
  std::unordered_map<std::string, execution::ast::Identifier> str_to_ident_;
};

}  // namespace udf
}  // namespace compiler
}  // namespace execution
}  // namespace noisepage
