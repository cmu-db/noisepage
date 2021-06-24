#pragma once

namespace noisepage {
namespace execution {
namespace ast {
namespace udf {

class AbstractAST;
class StmtAST;
class ExprAST;
class ValueExprAST;
class IsNullExprAST;
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
class FunctionAST;

/**
 * The ASTNodeVisitor class defines the interface for
 * visitors of the UDF abstract syntax tree.
 */
class ASTNodeVisitor {
 public:
  /**
   * Destroy the visitor.
   */
  virtual ~ASTNodeVisitor() = default;

  /**
   * Visit an AbstractAST node.
   * @param ast The node to visit
   */
  virtual void Visit(AbstractAST *ast) = 0;

  /**
   * Visit an StmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(StmtAST *ast) = 0;

  /**
   * Visit an ExprAST node.
   * @param ast The node to visit
   */
  virtual void Visit(ExprAST *ast) = 0;

  /**
   * Visit an FunctionAST node.
   * @param ast The node to visit
   */
  virtual void Visit(FunctionAST *ast) = 0;

  /**
   * Visit an ValueExprAST node.
   * @param ast The node to visit
   */
  virtual void Visit(ValueExprAST *ast) = 0;

  /**
   * Visit an VariableExprAST node.
   * @param ast The node to visit
   */
  virtual void Visit(VariableExprAST *ast) = 0;

  /**
   * Visit an BinaryExprAST node.
   * @param ast The node to visit
   */
  virtual void Visit(BinaryExprAST *ast) = 0;

  /**
   * Visit an IsNullExprAST node.
   * @param ast The node to visit
   */
  virtual void Visit(IsNullExprAST *ast) = 0;

  /**
   * Visit an CallExprAST node.
   * @param ast The node to visit
   */
  virtual void Visit(CallExprAST *ast) = 0;

  /**
   * Visit an MemberExprAST node.
   * @param ast The node to visit
   */
  virtual void Visit(MemberExprAST *ast) = 0;

  /**
   * Visit an SeqStmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(SeqStmtAST *ast) = 0;

  /**
   * Visit an DeclStmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(DeclStmtAST *ast) = 0;

  /**
   * Visit an IfStmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(IfStmtAST *ast) = 0;

  /**
   * Visit an WhileStmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(WhileStmtAST *ast) = 0;

  /**
   * Visit an RetStmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(RetStmtAST *ast) = 0;

  /**
   * Visit an AssignStmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(AssignStmtAST *ast) = 0;

  /**
   * Visit an ForStmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(ForStmtAST *ast) = 0;

  /**
   * Visit an SQLStmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(SQLStmtAST *ast) = 0;

  /**
   * Visit an DynamicSQLStmtAST node.
   * @param ast The node to visit
   */
  virtual void Visit(DynamicSQLStmtAST *ast) = 0;
};

}  // namespace udf
}  // namespace ast
}  // namespace execution
}  // namespace noisepage
