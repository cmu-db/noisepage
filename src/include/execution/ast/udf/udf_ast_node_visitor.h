#pragma once

// TODO(Kyle): This whole file needs documentation.

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

  virtual void Visit(AbstractAST *){};
  virtual void Visit(StmtAST *){};
  virtual void Visit(ExprAST *){};
  virtual void Visit(FunctionAST *){};
  virtual void Visit(ValueExprAST *){};
  virtual void Visit(VariableExprAST *){};
  virtual void Visit(BinaryExprAST *){};
  virtual void Visit(IsNullExprAST *){};
  virtual void Visit(CallExprAST *){};
  virtual void Visit(MemberExprAST *){};
  virtual void Visit(SeqStmtAST *){};
  virtual void Visit(DeclStmtAST *){};
  virtual void Visit(IfStmtAST *){};
  virtual void Visit(WhileStmtAST *){};
  virtual void Visit(RetStmtAST *){};
  virtual void Visit(AssignStmtAST *){};
  virtual void Visit(ForStmtAST *){};
  virtual void Visit(SQLStmtAST *){};
  virtual void Visit(DynamicSQLStmtAST *){};
};
}  // namespace udf
}  // namespace ast
}  // namespace execution
}  // namespace noisepage
