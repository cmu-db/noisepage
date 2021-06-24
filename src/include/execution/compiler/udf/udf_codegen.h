#pragma once

#include <string>
#include <unordered_map>

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

/**
 * The UDFCodegen class implements a visitor for UDF AST nodes
 * and encapsulates all of the logic required to generate code
 * from the UDF abstract syntax tree.
 */
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

  /**
   * Destroy the UDF code generation context.
   */
  ~UDFCodegen() override = default;

  /**
   * Generate a UDF from the given abstract syntax tree.
   * @param ast The AST from which to generate the UDF
   */
  void GenerateUDF(ast::udf::AbstractAST *ast);

  /**
   * Visit an AbstractAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::AbstractAST *ast) override;

  /**
   * Visit a FunctionAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::FunctionAST *ast) override;

  /**
   * Visit a StmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::StmtAST *ast) override;

  /**
   * Visit an ExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::ExprAST *ast) override;

  /**
   * Visit a ValueExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::ValueExprAST *ast) override;

  /**
   * Visit a VariableExprAST node.
   */
  void Visit(ast::udf::VariableExprAST *ast) override;

  /**
   * Visit a BinaryExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::BinaryExprAST *ast) override;

  /**
   * Visit a CallExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::CallExprAST *ast) override;

  /**
   * Visit an IsNullExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::IsNullExprAST *ast) override;

  /**
   * Visit a SeqStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::SeqStmtAST *ast) override;

  /**
   * Visit a DeclStmtNode node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::DeclStmtAST *ast) override;

  /**
   * Visit a IfStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::IfStmtAST *ast) override;

  /**
   * Visit a WhileStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::WhileStmtAST *ast) override;

  /**
   * Visit a RetStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::RetStmtAST *ast) override;

  /**
   * Visit an AssignStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::AssignStmtAST *ast) override;

  /**
   * Visit a SQLStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::SQLStmtAST *ast) override;

  /**
   * Visit a DynamicSQLStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::DynamicSQLStmtAST *ast) override;

  /**
   * Visit a ForStmtAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::ForStmtAST *ast) override;

  /**
   * Visit a MemberExprAST node.
   * @param ast The AST node to visit
   */
  void Visit(ast::udf::MemberExprAST *ast) override;

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

  /** The catalog access used during code generation */
  catalog::CatalogAccessor *accessor_;

  /** The function builder used during code generation */
  FunctionBuilder *fb_;

  /** The AST context for the UDF */
  ast::udf::UDFASTContext *udf_ast_context_;

  /** The code generation instance */
  CodeGen *codegen_;

  /** The OID of the relevant database */
  catalog::db_oid_t db_oid_;

  /** Auxiliary declarations */
  execution::util::RegionVector<execution::ast::Decl *> aux_decls_;

  /** Flag indicating whether this UDF requires an execution context */
  bool needs_exec_ctx_;

  /** The current type during code generation */
  type::TypeId current_type_{type::TypeId::INVALID};

  /** The destination expression */
  execution::ast::Expr *dst_;

  /** Map from human-readable string identifier to internal identifier */
  std::unordered_map<std::string, execution::ast::Identifier> str_to_ident_;
};

}  // namespace udf
}  // namespace compiler
}  // namespace execution
}  // namespace noisepage
