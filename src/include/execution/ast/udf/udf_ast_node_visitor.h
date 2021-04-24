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

class ASTNodeVisitor {
 public:
  virtual ~ASTNodeVisitor(){};
  virtual void Visit(AbstractAST *ast) = 0;
  virtual void Visit(StmtAST *ast) = 0;
  virtual void Visit(ExprAST *ast) = 0;
  virtual void Visit(FunctionAST *ast) = 0;
  virtual void Visit(ValueExprAST *ast) = 0;
  virtual void Visit(VariableExprAST *ast) = 0;
  virtual void Visit(BinaryExprAST *ast) = 0;
  virtual void Visit(IsNullExprAST *ast) = 0;
  virtual void Visit(CallExprAST *ast) = 0;
  virtual void Visit(MemberExprAST *ast) = 0;
  virtual void Visit(SeqStmtAST *ast) = 0;
  virtual void Visit(DeclStmtAST *ast) = 0;
  virtual void Visit(IfStmtAST *ast) = 0;
  virtual void Visit(WhileStmtAST *ast) = 0;
  virtual void Visit(RetStmtAST *ast) = 0;
  virtual void Visit(AssignStmtAST *ast) = 0;
  virtual void Visit(ForStmtAST *ast) = 0;
  virtual void Visit(SQLStmtAST *ast) = 0;
  virtual void Visit(DynamicSQLStmtAST *ast) = 0;
};

}  // namespace udf
}  // namespace ast
}  // namespace execution
}  // namespace noisepage
