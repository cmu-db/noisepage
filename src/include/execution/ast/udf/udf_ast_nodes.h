#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "parser/expression/constant_value_expression.h"
#include "parser/expression_defs.h"
#include "type/type_id.h"

#include "execution/ast/udf/udf_ast_node_visitor.h"
#include "execution/sql/value.h"

namespace noisepage::execution::ast::udf {

/**
 * The AbstractAST class serves as a base class for all AST nodes.
 */
class AbstractAST {
 public:
  /**
   * Destroy the AST node.
   */
  virtual ~AbstractAST() = default;

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  virtual void Accept(ASTNodeVisitor *visitor) { visitor->Visit(this); }
};

/**
 * The StmtAST class serves as the base class for all statement nodes.
 */
class StmtAST : public AbstractAST {
 public:
  /**
   * Destroy the AST node.
   */
  ~StmtAST() override = default;

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

/**
 * The ExprAST class serves as the base class for all expression nodes.
 */
class ExprAST : public StmtAST {
 public:
  /**
   * Destroy the AST node.
   */
  ~ExprAST() override = default;

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }
};

/**
 * The ValueExprAST class represents literal values.
 */
class ValueExprAST : public ExprAST {
 public:
  /**
   * Construct a new ValueExprAST instance.
   * @param value The AbstractExpression that represents the value
   */
  explicit ValueExprAST(std::unique_ptr<parser::AbstractExpression> &&value) : value_(std::move(value)) {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return A mutable pointer to the value expression */
  parser::AbstractExpression *Value() { return value_.get(); }

  /** @return An immutable pointer to the value expression */
  const parser::AbstractExpression *Value() const { return value_.get(); }

 private:
  /** The expression that represents the value */
  std::unique_ptr<parser::AbstractExpression> value_;
};

/**
 * The IsNullExprAST class represents an expression that performs a NULL check.
 */
class IsNullExprAST : public ExprAST {
 public:
  /**
   * Construct a new IsNullExprAST instance.
   * @param is_null_check The NULL check flag
   * @param child The child expression
   */
  IsNullExprAST(bool is_null_check, std::unique_ptr<ExprAST> &&child)
      : is_null_check_{is_null_check}, child_{std::move(child)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return `true` if the NULL check is performed, `false` otherwise */
  bool IsNullCheck() const { return is_null_check_; }

  /** @return The child expression */
  ExprAST *Child() { return child_.get(); }

  /** @return The child expression */
  const ExprAST *Child() const { return child_.get(); }

 private:
  /** The NULL check flag */
  bool is_null_check_;

  /** The child expression */
  std::unique_ptr<ExprAST> child_;
};

/**
 * The VariableExprAST class represents an expression that references a variable.
 */
class VariableExprAST : public ExprAST {
 public:
  /**
   * Construct a new VariableExprAST instance.
   * @param name The name of the variable
   */
  explicit VariableExprAST(std::string name) : name_{std::move(name)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The name of the variable */
  const std::string &Name() const { return name_; }

 private:
  /** The name of the variable */
  const std::string name_;
};

/**
 * The MemberExprAST class represents a structure member expression.
 */
class MemberExprAST : public ExprAST {
 public:
  /**
   * Construct a new MemberExprAST instance.
   * @param object The structure
   * @param field The name of the field in the structure
   */
  MemberExprAST(std::unique_ptr<VariableExprAST> &&object, std::string field)
      : object_{std::move(object)}, field_(std::move(field)) {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The object */
  VariableExprAST *Object() { return object_.get(); }

  /** @return The object */
  const VariableExprAST *Object() const { return object_.get(); }

  /** @return The name of the field */
  const std::string &FieldName() const { return field_; }

 private:
  /** The expression for the object */
  std::unique_ptr<VariableExprAST> object_;

  /** The identifier for the field in the object */
  std::string field_;
};

/**
 * The BinaryExprAST class represents a generic binary expression.
 */
class BinaryExprAST : public ExprAST {
 public:
  /**
   * Construct a new BinaryExprAST instance.
   * @param op The expression type for the operation
   * @param lhs The expression on the left-hande side of the operation
   * @param rhs The expression on the right-hand side of the operation
   */
  BinaryExprAST(parser::ExpressionType op, std::unique_ptr<ExprAST> &&lhs, std::unique_ptr<ExprAST> &&rhs)
      : op_{op}, lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The expression type for the operation */
  parser::ExpressionType Op() const { return op_; }

  /** @return A mutable pointer to the left expression */
  ExprAST *Left() { return lhs_.get(); }

  /** @return An immutable pointer to the left expression */
  const ExprAST *Left() const { return lhs_.get(); }

  /** @return A mutable pointer to the right expression */
  ExprAST *Right() { return rhs_.get(); }

  /** @return An immutable pointer to the right expression */
  const ExprAST *Right() const { return rhs_.get(); }

 private:
  /** The expression type for the operation */
  parser::ExpressionType op_;

  /** The expression on the left-hand side of the operation */
  std::unique_ptr<ExprAST> lhs_;

  /** The expression on the right-hand side of the operation */
  std::unique_ptr<ExprAST> rhs_;
};

/**
 * The CallExprAST class represents a function call expression.
 */
class CallExprAST : public ExprAST {
 public:
  /**
   * Construct a new CallExprAST instance.
   * @param callee The name of the called function
   * @param args The arguments to the function call
   */
  CallExprAST(std::string callee, std::vector<std::unique_ptr<ExprAST>> &&args)
      : callee_{std::move(callee)}, args_{std::move(args)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The name of the called function */
  const std::string &Callee() const { return callee_; }

  /** @return A mutable reference to the function call arguments */
  std::vector<std::unique_ptr<ExprAST>> &Args() { return args_; }

  /** @return An immutable reference to the function call arguments */
  const std::vector<std::unique_ptr<ExprAST>> &Args() const { return args_; }

 private:
  /** The name of the called function */
  const std::string callee_;

  /** The arguments to the function call */
  std::vector<std::unique_ptr<ExprAST>> args_;
};

/**
 * The SeqStmtAST class represents a sequence of statements.
 */
class SeqStmtAST : public StmtAST {
 public:
  /**
   * Construct a new SeqStmtAST instance.
   * @param statements The collection of statements in the sequence
   */
  explicit SeqStmtAST(std::vector<std::unique_ptr<StmtAST>> &&statements) : statements_(std::move(statements)) {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return A mutable reference to the statements in the sequence */
  std::vector<std::unique_ptr<StmtAST>> &Statements() { return statements_; }

  /** @return An immutable reference to the statements in the sequence */
  const std::vector<std::unique_ptr<StmtAST>> &Statements() const { return statements_; }

 private:
  /** The collection of statements in the sequence */
  std::vector<std::unique_ptr<StmtAST>> statements_;
};

// DeclStmtAST - Statement class for sequence of statements
/**
 * The DeclStmtAST class represents a declaration statement.
 */
class DeclStmtAST : public StmtAST {
 public:
  /**
   * Construct a new DeclStmtAST instance.
   * @param name The name of the variable that is declared
   * @param type The type of the declared variable
   * @param initial The initial value in the declaration
   */
  DeclStmtAST(std::string name, type::TypeId type, std::unique_ptr<ExprAST> &&initial)
      : name_{std::move(name)}, type_(type), initial_{std::move(initial)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  /** @return The name of the declared variable */
  const std::string &Name() const { return name_; }

  /** @return The type of the declared variable */
  type::TypeId Type() const { return type_; }

  /** @return A mutable pointer to the initial value expression */
  ExprAST *Initial() { return initial_.get(); }

  /** @return An immutable pointer to the initial value expression */
  const ExprAST *Initial() const { return initial_.get(); }

 private:
  /** The name of the variable declared in the statement */
  std::string name_;

  /** The type of the declared variable */
  type::TypeId type_;

  /** The initial value of the declaration */
  std::unique_ptr<ExprAST> initial_;
};

/**
 * The IfStmtAST class represents an IF/THEN/ELSE construct.
 */
class IfStmtAST : public StmtAST {
 public:
  /**
   * Construct a new IfStmtAST instance.
   * @param cond_expr The conditional expression
   * @param then_stmt The `then` statement
   * @param else_stmt The `else` statement
   */
  IfStmtAST(std::unique_ptr<ExprAST> &&cond_expr, std::unique_ptr<StmtAST> &&then_stmt,
            std::unique_ptr<StmtAST> &&else_stmt)
      : cond_expr_{std::move(cond_expr)}, then_stmt_{std::move(then_stmt)}, else_stmt_{std::move(else_stmt)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  /** @return The conditional expression */
  ExprAST *Condition() { return cond_expr_.get(); }

  /** @return The conditional expression */
  const ExprAST *Condition() const { return cond_expr_.get(); }

  /** @return The `then` statement */
  StmtAST *Then() { return then_stmt_.get(); }

  /** @return The `then` statement */
  const StmtAST *Then() const { return then_stmt_.get(); }

  /** @return The `else` statement */
  StmtAST *Else() { return else_stmt_.get(); }

  /** @return The `else` statement */
  const StmtAST *Else() const { return else_stmt_.get(); }

 private:
  /** The conditional expression */
  std::unique_ptr<ExprAST> cond_expr_;

  /** The `then` statement */
  std::unique_ptr<StmtAST> then_stmt_;

  /** The `else` statement */
  std::unique_ptr<StmtAST> else_stmt_;
};

/**
 * The ForIStmtAST class represents a `for`-loop construct.
 *
 * Ex: FOR i IN 1..10 LOOP...
 */
class ForIStmtAST : public StmtAST {
 public:
  /**
   * The default query that defines the "step" expression.
   *
   * The PLpgSQL documentation specifies this behavior.
   */
  constexpr static const char DEFAULT_STEP_EXPR[] = "SELECT 1";

  /**
   * Construct a new ForIStmtAST instance.
   * @param variables The collection of variables in the loop
   * @param body The body of the loop
   */
  ForIStmtAST(std::string variable, std::unique_ptr<ExprAST> lower, std::unique_ptr<ExprAST> upper,
              std::unique_ptr<ExprAST> step, std::unique_ptr<StmtAST> body)
      : variable_{std::move(variable)},
        lower_{std::move(lower)},
        upper_{std::move(upper)},
        step_{std::move(step)},
        body_{std::move(body)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  /** @return The loop variable */
  const std::string &Variable() const { return variable_; }

  /** @return A mutable pointer to the loop lower-bound expression */
  ExprAST *Lower() { return lower_.get(); }

  /** @return An immutable pointer to the loop lower-bound expression */
  const ExprAST *Lower() const { return lower_.get(); }

  /** @return A mutable pointer to the loop upper-bound expression */
  ExprAST *Upper() { return upper_.get(); }

  /** @return An immutable pointer to the loop upper-bound expression */
  const ExprAST *Upper() const { return upper_.get(); }

  /** @return A mutable pointer to the loop step expression */
  ExprAST *Step() { return step_.get(); }

  /** @return An immutable pointer to the loop step expression */
  const ExprAST *Step() const { return step_.get(); }

  /** @return A mutable pointer to the loop body statement */
  StmtAST *Body() { return body_.get(); }

  /** @return An immutable pointer to the loop body statement */
  const StmtAST *Body() const { return body_.get(); }

 private:
  /** The identifier for the loop variable */
  const std::string variable_;
  /** The expression that defines the loop lower-bound */
  std::unique_ptr<ExprAST> lower_;
  /** The expression that defines the loop upper-bound */
  std::unique_ptr<ExprAST> upper_;
  /** The expression that defines the loop step */
  std::unique_ptr<ExprAST> step_;
  /** The loop body  */
  std::unique_ptr<StmtAST> body_;
};

/**
 * The ForSStmtAST class represents a `for`-loop construct.
 *
 * Ex: FOR record IN (SELECT * FROM tmp) LOOP ...
 */
class ForSStmtAST : public StmtAST {
 public:
  /**
   * Construct a new ForSStmtAST instance.
   * @param variables The collection of variables in the loop
   * @param query The associated query
   * @param body The body of the loop
   */
  ForSStmtAST(std::vector<std::string> &&variables, std::unique_ptr<parser::ParseResult> &&query,
              std::unique_ptr<StmtAST> body)
      : variables_{std::move(variables)}, query_{std::move(query)}, body_{std::move(body)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); };

  /** @return The collection of loop variables */
  const std::vector<std::string> &Variables() const { return variables_; }

  /** @return The associated query */
  parser::ParseResult *Query() { return query_.get(); }

  /** @return The associated query */
  const parser::ParseResult *Query() const { return query_.get(); }

  /** @return The loop body statement */
  StmtAST *Body() { return body_.get(); }

  /** @return The loop body statement */
  const StmtAST *Body() const { return body_.get(); }

 private:
  /** The collection of loop variables */
  std::vector<std::string> variables_;

  /** The associated query */
  std::unique_ptr<parser::ParseResult> query_;

  /** The loop body statement */
  std::unique_ptr<StmtAST> body_;
};

/**
 * The WhileStmtAST represents a `while`-loop construct.
 */
class WhileStmtAST : public StmtAST {
 public:
  /**
   * Construct a new WhileStmtAST instance.
   * @param condition The loop condition
   * @param body The loop body statement
   */
  WhileStmtAST(std::unique_ptr<ExprAST> &&condition, std::unique_ptr<StmtAST> &&body)
      : condition_{std::move(condition)}, body_{std::move(body)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The loop condition */
  ExprAST *Condition() { return condition_.get(); }

  /** @return The loop condition */
  const ExprAST *Condition() const { return condition_.get(); }

  /** @return The loop body statement */
  StmtAST *Body() { return body_.get(); }

  /** @return The loop body statement */
  const StmtAST *Body() const { return body_.get(); }

 private:
  /** The loop condition */
  std::unique_ptr<ExprAST> condition_;

  /** The loop body statement */
  std::unique_ptr<StmtAST> body_;
};

/**
 * The RetStmtAST class represents a `return` statement.
 */
class RetStmtAST : public StmtAST {
 public:
  /**
   * Construct a new RetStmtAST instance.
   * @param ret_expr The `return` expression
   */
  explicit RetStmtAST(std::unique_ptr<ExprAST> &&ret_expr) : ret_expr_{std::move(ret_expr)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The `return` expression */
  ExprAST *Return() { return ret_expr_.get(); }

  /** @return The `return` expression */
  const ExprAST *Return() const { return ret_expr_.get(); }

 private:
  /** The `return` expression */
  std::unique_ptr<ExprAST> ret_expr_;
};

/**
 * The AssignStmtAST class represents an assignment statement.
 */
class AssignStmtAST : public ExprAST {
 public:
  /**
   * Construct a new AssignStmtAST instance.
   * @param dst The variable that represents the destination of the assignment
   * @param src The expression that represents the source of the assignment
   */
  AssignStmtAST(std::unique_ptr<VariableExprAST> &&dst, std::unique_ptr<ExprAST> &&src)
      : dst_{std::move(dst)}, src_{std::move(src)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The destination variable of the assignment */
  VariableExprAST *Destination() { return dst_.get(); }

  /** @return The destination variable of the assignment */
  const VariableExprAST *Destination() const { return dst_.get(); }

  /** @return The source expression of the assignment */
  ExprAST *Source() { return src_.get(); }

  /** @return The source expression of the assignment */
  const ExprAST *Source() const { return src_.get(); }

 private:
  /** The destination of the assignment */
  std::unique_ptr<VariableExprAST> dst_;

  /** The source of the assignment */
  std::unique_ptr<ExprAST> src_;
};

/**
 * The SQLStmtAST class represents a SQL statement.
 */
class SQLStmtAST : public StmtAST {
 public:
  /**
   * Construct a new SQLStmtAST instance.
   * @param query The result of parsing the SQL query
   * @param variables The collection of identifiers of variables
   * to which results of the query are bound
   */
  SQLStmtAST(std::unique_ptr<parser::ParseResult> &&query, std::vector<std::string> &&variables)
      : query_{std::move(query)}, variables_{std::move(variables)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The result of parsing the SQL query */
  parser::ParseResult *Query() { return query_.get(); }

  /** @return The result of parsing the SQL query */
  const parser::ParseResult *Query() const { return query_.get(); }

  /** @return The variable names to which results are bound */
  const std::vector<std::string> &Variables() const { return variables_; }

 private:
  /** The result of parsing the SQL query */
  std::unique_ptr<parser::ParseResult> query_;

  /** The names of the variables to which results are bound */
  std::vector<std::string> variables_;
};

/**
 * The DynamicSQLStmtAST class represents a dynamic SQL statement.
 */
class DynamicSQLStmtAST : public StmtAST {
 public:
  /**
   * Construct a new DynamicSQLStmtAST instance.
   * @param query The expression that represents the query
   * @param name The name of the variable to which results are bound
   */
  DynamicSQLStmtAST(std::unique_ptr<ExprAST> &&query, std::string name)
      : query_{std::move(query)}, name_{std::move(name)} {}

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The expression that represents the query */
  const ExprAST *Query() const { return query_.get(); }

  /** @return The name of the variable to which results are bound */
  const std::string &Name() const { return name_; }

 private:
  /** The expression that represents the query */
  std::unique_ptr<ExprAST> query_;

  /** The name of the variable to which results are bound */
  std::string name_;
};

/**
 * The FunctionAST class represents a function definition.
 */
class FunctionAST : public AbstractAST {
 public:
  /**
   * Construct a new FunctionAST instance.
   * @param body The body of the function
   * @param parameter_names The names of the parameters to the function
   * @param parameter_types The types of the parameters to the function
   */
  FunctionAST(std::unique_ptr<StmtAST> &&body, std::vector<std::string> parameter_names,
              std::vector<type::TypeId> parameter_types)
      : body_{std::move(body)},
        parameter_names_{std::move(parameter_names)},
        parameter_types_{std::move(parameter_types)} {
    NOISEPAGE_ASSERT(parameter_names_.size() == parameter_types_.size(), "Parameter Name and Type Mismatch");
    // TODO(Kyle): The copies made in this constructor may not be necessary,
    // I need to look more closely at the ownership for this data
  }

  /**
   * AST visitor pattern.
   * @param visitor The visitor
   */
  void Accept(ASTNodeVisitor *visitor) override { visitor->Visit(this); }

  /** @return The function body */
  StmtAST *Body() { return body_.get(); }

  /** @return The function body */
  const StmtAST *Body() const { return body_.get(); }

  /** The function parameter names */
  const std::vector<std::string> &ParameterNames() const { return parameter_names_; }

  /** @return The function parameter types */
  const std::vector<type::TypeId> &ParameterTypes() const { return parameter_types_; }

 private:
  /** The body of the function */
  std::unique_ptr<StmtAST> body_;

  /** The names of the parameters to the function */
  std::vector<std::string> parameter_names_;

  /** The types of the parameters to the function */
  std::vector<type::TypeId> parameter_types_;
};

// ----------------------------------------------------------------------------
// Error Handling Helpers
// ----------------------------------------------------------------------------

std::unique_ptr<ExprAST> LogError(const char *str);

}  // namespace noisepage::execution::ast::udf
