#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "parser/expression/constant_value_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

#include "execution/ast/udf/udf_ast_node_visitor.h"
#include "execution/sql/value.h"

namespace noisepage {
namespace execution {
namespace ast {
namespace udf {

// AbstractAST - Base class for all AST nodes.
class AbstractAST {
 public:
  virtual ~AbstractAST() = default;

  virtual void Accept(ASTNodeVisitor *visitor) { visitor->Visit(this); }
};

// StmtAST - Base class for all statement nodes.
class StmtAST : public AbstractAST {
 public:
  virtual ~StmtAST() = default;

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// ExprAST - Base class for all expression nodes.
class ExprAST : public StmtAST {
 public:
  virtual ~ExprAST() = default;

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// DoubleExprAST - Expression class for numeric literals like "1.1".
class ValueExprAST : public ExprAST {
 public:
  std::unique_ptr<parser::AbstractExpression> value_;

  explicit ValueExprAST(std::unique_ptr<parser::AbstractExpression> value) : value_(std::move(value)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

class IsNullExprAST : public ExprAST {
 public:
  bool is_null_check_;
  std::unique_ptr<ExprAST> child_;

  IsNullExprAST(bool is_null_check, std::unique_ptr<ExprAST> child)
      : is_null_check_(is_null_check), child_(std::move(child)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// VariableExprAST - Expression class for referencing a variable, like "a".
class VariableExprAST : public ExprAST {
 public:
  std::string name;

  explicit VariableExprAST(const std::string &name) : name(name) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// VariableExprAST - Expression class for referencing a variable, like "a".
class MemberExprAST : public ExprAST {
 public:
  std::unique_ptr<VariableExprAST> object;
  std::string field;

  MemberExprAST(std::unique_ptr<VariableExprAST> &&object, std::string field)
      : object(std::move(object)), field(field) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// BinaryExprAST - Expression class for a binary operator.
class BinaryExprAST : public ExprAST {
 public:
  parser::ExpressionType op;
  std::unique_ptr<ExprAST> lhs, rhs;

  BinaryExprAST(parser::ExpressionType op, std::unique_ptr<ExprAST> lhs, std::unique_ptr<ExprAST> rhs)
      : op(op), lhs(std::move(lhs)), rhs(std::move(rhs)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// CallExprAST - Expression class for function calls.
class CallExprAST : public ExprAST {
 public:
  std::string callee;
  std::vector<std::unique_ptr<ExprAST>> args;

  CallExprAST(const std::string &callee, std::vector<std::unique_ptr<ExprAST>> args)
      : callee(callee), args(std::move(args)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// SeqStmtAST - Statement class for sequence of statements
class SeqStmtAST : public StmtAST {
 public:
  std::vector<std::unique_ptr<StmtAST>> stmts;

  explicit SeqStmtAST(std::vector<std::unique_ptr<StmtAST>>&& stmts) : stmts(std::move(stmts)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// DeclStmtAST - Statement class for sequence of statements
class DeclStmtAST : public StmtAST {
 public:
  std::string name;
  type::TypeId type;
  std::unique_ptr<ExprAST> initial;

  DeclStmtAST(std::string name, type::TypeId type, std::unique_ptr<ExprAST> initial)
      : name(std::move(name)), type(std::move(type)), initial(std::move(initial)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };
};

// IfStmtAST - Statement class for if/then/else.
class IfStmtAST : public StmtAST {
 public:
  std::unique_ptr<ExprAST> cond_expr;
  std::unique_ptr<StmtAST> then_stmt, else_stmt;

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  IfStmtAST(std::unique_ptr<ExprAST> cond_expr, std::unique_ptr<StmtAST> then_stmt, std::unique_ptr<StmtAST> else_stmt)
      : cond_expr(std::move(cond_expr)), then_stmt(std::move(then_stmt)), else_stmt(std::move(else_stmt)) {}
};

class ForStmtAST : public StmtAST {
 public:
  std::vector<std::string> vars_;
  std::unique_ptr<parser::ParseResult> query_;
  std::unique_ptr<StmtAST> body_stmt_;

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  ForStmtAST(std::vector<std::string> &&vars_vec, std::unique_ptr<parser::ParseResult> query,
             std::unique_ptr<StmtAST> body_stmt)
      : vars_(std::move(vars_vec)), query_(std::move(query)), body_stmt_(std::move(body_stmt)) {}
};

// WhileAST - Statement class for while loop
class WhileStmtAST : public StmtAST {
 public:
  std::unique_ptr<ExprAST> cond_expr;
  std::unique_ptr<StmtAST> body_stmt;

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  WhileStmtAST(std::unique_ptr<ExprAST> cond_expr, std::unique_ptr<StmtAST> body_stmt)
      : cond_expr(std::move(cond_expr)), body_stmt(std::move(body_stmt)) {}
};

// RetStmtAST - Statement class for sequence of statements
class RetStmtAST : public StmtAST {
 public:
  std::unique_ptr<ExprAST> expr;

  explicit RetStmtAST(std::unique_ptr<ExprAST> expr) : expr(std::move(expr)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// AssignStmtAST - Expression class for a binary operator.
class AssignStmtAST : public ExprAST {
 public:
  std::unique_ptr<VariableExprAST> lhs;
  std::unique_ptr<ExprAST> rhs;

  AssignStmtAST(std::unique_ptr<VariableExprAST> lhs, std::unique_ptr<ExprAST> rhs)
      : lhs(std::move(lhs)), rhs(std::move(rhs)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// SQLStmtAST - Expression class for a SQL Statement.
class SQLStmtAST : public StmtAST {
 public:
  std::unique_ptr<parser::ParseResult> query;
  std::string var_name;
  std::unordered_map<std::string, std::pair<std::string, size_t>> udf_params;

  SQLStmtAST(std::unique_ptr<parser::ParseResult> query, std::string var_name,
             std::unordered_map<std::string, std::pair<std::string, size_t>> &&udf_params)
      : query(std::move(query)), var_name(std::move(var_name)), udf_params(std::move(udf_params)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// DynamicSQLStmtAST - Expression class for a SQL Statement.
class DynamicSQLStmtAST : public StmtAST {
 public:
  std::unique_ptr<ExprAST> query;
  std::string var_name;

  DynamicSQLStmtAST(std::unique_ptr<ExprAST> query, std::string var_name)
      : query(std::move(query)), var_name(std::move(var_name)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// FunctionAST - This class represents a function definition itself.
class FunctionAST : public AbstractAST {
 public:
  std::unique_ptr<StmtAST> body;
  std::vector<std::string> param_names_;
  std::vector<type::TypeId> param_types_;

  FunctionAST(std::unique_ptr<StmtAST> body, std::vector<std::string> &&param_names,
              std::vector<type::TypeId> &&param_types)
      : body(std::move(body)), param_names_(std::move(param_names)), param_types_(std::move(param_types)) {}

  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

// ----------------------------------------------------------------------------
// Error Handling Helpers
// ----------------------------------------------------------------------------

std::unique_ptr<ExprAST> LogError(const char *str);

}  // namespace udf
}  // namespace ast
}  // namespace execution
}  // namespace noisepage
