#pragma once

#include <cstdint>
#include <utility>

#include "llvm/Support/Casting.h"

#include "common/strong_typedef.h"
#include "execution/ast/identifier.h"
#include "execution/parsing/token.h"
#include "execution/util/execution_common.h"
#include "execution/util/region.h"
#include "execution/util/region_containers.h"

namespace terrier::execution {

namespace sema {
class Sema;
}  // namespace sema

namespace ast {

// Top-level file node
#define FILE_NODE(T) T(File)

/**
 * All possible declaration types.
 * NOTE: If you add a new declaration node to either the beginning or end of
 * the list, remember to modify Decl::classof() to update the bounds check.
 */
#define DECLARATION_NODES(T) \
  T(FieldDecl)               \
  T(FunctionDecl)            \
  T(StructDecl)              \
  T(VariableDecl)

/**
 * All possible statements
 * NOTE: If you add a new statement node to either the beginning or end of the
 * list, remember to modify Stmt::classof() to update the bounds check.
 */
#define STATEMENT_NODES(T) \
  T(AssignmentStmt)        \
  T(BlockStmt)             \
  T(DeclStmt)              \
  T(ExpressionStmt)        \
  T(ForStmt)               \
  T(ForInStmt)             \
  T(IfStmt)                \
  T(ReturnStmt)

/**
 * All possible expressions
 * NOTE: If you add a new expression node to either the beginning or end of the
 * list, remember to modify Expr::classof() to update the bounds check.
 */
#define EXPRESSION_NODES(T)             \
  T(BadExpr)                            \
  T(BinaryOpExpr)                       \
  T(CallExpr)                           \
  T(ComparisonOpExpr)                   \
  T(FunctionLitExpr)                    \
  T(IdentifierExpr)                     \
  T(ImplicitCastExpr)                   \
  T(IndexExpr)                          \
  T(LitExpr)                            \
  T(MemberExpr)                         \
  T(UnaryOpExpr)                        \
  /* Type Representation Expressions */ \
  T(ArrayTypeRepr)                      \
  T(FunctionTypeRepr)                   \
  T(MapTypeRepr)                        \
  T(PointerTypeRepr)                    \
  T(StructTypeRepr)

/**
 * All AST nodes
 */
#define AST_NODES(T)   \
  DECLARATION_NODES(T) \
  EXPRESSION_NODES(T)  \
  FILE_NODE(T)         \
  STATEMENT_NODES(T)

/**
 * Forward declare some base classes
 */
class Decl;
class Expr;
class Stmt;
class Type;

/**
 * Forward declare all nodes
 */
#define FORWARD_DECLARE(name) class name;
AST_NODES(FORWARD_DECLARE)
#undef FORWARD_DECLARE

// ---------------------------------------------------------
// AST Node

// ---------------------------------------------------------
/**
 * The base class for all AST nodes. AST nodes are emphemeral, and thus, are
 * only allocated from regions. Once created, they are immutable only during
 * semantic checks - this is why you'll often see sema::Sema declared as a
 * friend class in some concrete node subclasses.
 *
 * All nodes have a "kind" that represents as an ID indicating the specific
 * kind of AST node it is (i.e., if it's an if-statement or a binary
 * expression). You can query the node for it's kind, but it's usually more
 * informative and clear to use the Is() method.
 */
class AstNode : public util::RegionObject {
 public:
#define T(kind) kind,
  /**
   * The kind enumeration listing all possible node kinds
   */
  enum class Kind : uint8_t { AST_NODES(T) };
#undef T

  /**
   * @return the kind of this node
   */
  Kind GetKind() const { return kind_; }

  /**
   * @return the position in the source where this element was found
   */
  const SourcePosition &Position() const { return pos_; }

  /**
   * @return the name of this node. NOTE: this is mainly used in tests!
   */
  const char *KindName() const {
#define KIND_CASE(kind) \
  case Kind::kind:      \
    return #kind;

    // Main type switch
    // clang-format off
    switch (GetKind()) {
      default: { UNREACHABLE("Impossible kind name"); }
      AST_NODES(KIND_CASE)
    }
      // clang-format on
#undef KIND_CASE
  }

  /**
   * Checks if this node is an instance of the specified class
   * @tparam T class to check
   * @return true if the node is an instance of the class
   */
  template <typename T>
  bool Is() const {
    return llvm::isa<T>(this);
  }

  /**
   * Casts this node to an instance of the specified class, asserting if the
   * conversion is invalid. This is probably most similar to std::static_cast<>
   * or std::reinterpret_cast<>
   * @tparam T type to cast to
   * @return casted node
   */
  template <typename T>
  T *As() {
    TERRIER_ASSERT(Is<T>(), "Using unsafe cast on mismatched node types");
    return reinterpret_cast<T *>(this);
  }

  /**
   * Casts this node to an instance of the specified class, asserting if the
   * conversion is invalid. This is probably most similar to std::static_cast<>
   * or std::reinterpret_cast<>
   * @tparam T type to cast to
   * @return casted node
   */
  template <typename T>
  const T *As() const {
    TERRIER_ASSERT(Is<T>(), "Using unsafe cast on mismatched node types");
    return reinterpret_cast<const T *>(this);
  }

  /**
   * Casts this node to an instance of the provided class if valid. If the
   * conversion is invalid, this returns a NULL pointer. This is most similar to
   * std::dynamic_cast<T>, i.e., it's a checked cast.
   * @tparam T type to cast to
   * @return casted node
   */
  template <typename T>
  T *SafeAs() {
    return (Is<T>() ? As<T>() : nullptr);
  }

  /**
   * Casts this node to an instance of the provided class if valid. If the
   * conversion is invalid, this returns a NULL pointer. This is most similar to
   * std::dynamic_cast<T>, i.e., it's a checked cast.
   * @tparam T type to cast to
   * @return casted node
   */
  template <typename T>
  const T *SafeAs() const {
    return (Is<T>() ? As<T>() : nullptr);
  }

// Whether this node is of a certain kind
#define F(kind) \
  bool Is##kind() const { return Is<kind>(); }
  AST_NODES(F)
#undef F

 protected:
  /**
   * Private constructor
   * @param kind kind of the node
   * @param pos source code position
   */
  AstNode(Kind kind, const SourcePosition &pos) : kind_(kind), pos_(pos) {}

 private:
  // The kind of AST node
  Kind kind_;

  // The position in the original source where this node's underlying
  // information was found
  const SourcePosition pos_;
};

/**
 * Represents a file
 */
class File : public AstNode {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param decls list of top level declarations
   */
  File(const SourcePosition &pos, util::RegionVector<Decl *> &&decls)
      : AstNode(Kind::File, pos), decls_(std::move(decls)) {}

  /**
   * @return Returns the list of declarations
   */
  util::RegionVector<Decl *> &Declarations() { return decls_; }

  /**
   * Checks whether the given node is a File
   * @param node node to check
   * @return true iff given node is a File
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::File;
  }

 private:
  util::RegionVector<Decl *> decls_;
};

// ---------------------------------------------------------
// Declaration Nodes
// ---------------------------------------------------------

/**
 * Base class for all declarations in TPL. All declarations have a name, and
 * an optional type representation. Structure and function declarations have an
 * explicit type, but variables may not.
 */
class Decl : public AstNode {
 public:
  /**
   * Constructor
   * @param kind kind of declaration
   * @param pos source position
   * @param name declared identifier
   * @param type_repr type representation
   */
  Decl(Kind kind, const SourcePosition &pos, Identifier name, Expr *type_repr)
      : AstNode(kind, pos), name_(name), type_repr_(type_repr) {}

  /**
   * @return the name of the declared object
   */
  Identifier Name() const { return name_; }

  /**
   * @return the type representation.
   */
  Expr *TypeRepr() const { return type_repr_; }

  /**
   * Checks whether the given node is a Declaration.
   * @param node node to check
   * @return true iff given node is a Declaration
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() >= Kind::FieldDecl && node->GetKind() <= Kind::VariableDecl;
  }

 private:
  Identifier name_;
  Expr *type_repr_;
};

/**
 * A generic declaration of a function argument or a field in a struct
 */
class FieldDecl : public Decl {
 public:
  /**
   * Constructor
   * @param pos position in source
   * @param name identifier
   * @param type_repr type representation
   */
  FieldDecl(const SourcePosition &pos, Identifier name, Expr *type_repr)
      : Decl(Kind::FieldDecl, pos, name, type_repr) {}

  /**
   * Checks whether the given node is a FieldDecl.
   * @param node node to check
   * @return true iff given node is a FieldDecl
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::FieldDecl;
  }
};

/**
 * A function declaration
 */
class FunctionDecl : public Decl {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param name identifier
   * @param func function literal (param types, return type, body)
   */
  FunctionDecl(const SourcePosition &pos, Identifier name, FunctionLitExpr *func);

  /**
   * @return Return the function literal
   */
  FunctionLitExpr *Function() const { return func_; }

  /**
   * Checks whether the given node is a FunctionDecl.
   * @param node node to check
   * @return true iff given node is a FunctionDecl
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::FunctionDecl;
  }

 private:
  FunctionLitExpr *func_;
};

/**
 * A structure declaration
 */
class StructDecl : public Decl {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param name identifier
   * @param type_repr struct type representation (list of fields).
   */
  StructDecl(const SourcePosition &pos, Identifier name, StructTypeRepr *type_repr);

  /**
   * Checks whether the given node is a Declaration.
   * @param node node to check
   * @return true iff given node is a Declaration
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::StructDecl;
  }
};

/**
 * A variable declaration
 */
class VariableDecl : public Decl {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param name identifer
   * @param type_repr type representation
   * @param init initial value
   */
  VariableDecl(const SourcePosition &pos, Identifier name, Expr *type_repr, Expr *init)
      : Decl(Kind::VariableDecl, pos, name, type_repr), init_(init) {}

  /**
   * @return Initial value
   */
  Expr *Initial() const { return init_; }

  /**
   * @return Did the variable declaration come with an explicit type i.e., var x:int = 0?
   */
  bool HasTypeDecl() const { return TypeRepr() != nullptr; }

  /**
   * @return Did the variable declaration come with an initial value?
   */
  bool HasInitialValue() const { return init_ != nullptr; }

  /**
   * Checks whether the given node is a VariableDecl.
   * @param node node to check
   * @return true iff given node is a VariableDecl.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::VariableDecl;
  }

 private:
  friend class sema::Sema;
  void SetInitial(ast::Expr *initial) { init_ = initial; }

 private:
  Expr *init_;
};

// ---------------------------------------------------------
// Statement Nodes
// ---------------------------------------------------------

/**
 * Base class for all statement nodes
 */
class Stmt : public AstNode {
 public:
  /**
   * Constructor
   * @param kind kind of statement
   * @param pos source position
   */
  Stmt(Kind kind, const SourcePosition &pos) : AstNode(kind, pos) {}

  /**
   * Determines if the given stmt, the last in a statement list, is terminating
   * @param stmt The statement node to check
   * @return True if statement has a terminator; false otherwise
   */
  static bool IsTerminating(Stmt *stmt);

  /**
   * Checks whether the given node is a Stmt.
   * @param node node to check
   * @return true iff given node is a Stmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() >= Kind::AssignmentStmt && node->GetKind() <= Kind::ReturnStmt;
  }
};

/**
 * An assignment, dest = source
 */
class AssignmentStmt : public Stmt {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param dest destination
   * @param src source
   */
  AssignmentStmt(const SourcePosition &pos, Expr *dest, Expr *src)
      : Stmt(AstNode::Kind::AssignmentStmt, pos), dest_(dest), src_(src) {}

  /**
   * @return the destination of the assignment
   */
  Expr *Destination() { return dest_; }

  /**
   * @return the source of the assignment
   */
  Expr *Source() { return src_; }

  /**
   * Checks whether the given node is a AssignmentStmt.
   * @param node node to check
   * @return true iff given node is a AssignmentStmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::AssignmentStmt;
  }

 private:
  friend class sema::Sema;

  // Used for implicit casts
  void SetSource(Expr *source) { src_ = source; }

 private:
  Expr *dest_;
  Expr *src_;
};

/**
 * A block of statements
 */
class BlockStmt : public Stmt {
 public:
  /**
   * Construct
   * @param pos source position
   * @param rbrace_pos position of right brace
   * @param statements list of statements within the block
   */
  BlockStmt(const SourcePosition &pos, const SourcePosition &rbrace_pos, util::RegionVector<Stmt *> &&statements)
      : Stmt(Kind::BlockStmt, pos), rbrace_pos_(rbrace_pos), statements_(std::move(statements)) {}

  /**
   * @return the list of statements
   */
  util::RegionVector<Stmt *> &Statements() { return statements_; }

  /**
   * @return the position of the right brace
   */
  const SourcePosition &RightBracePosition() const { return rbrace_pos_; }

  /**
   * Appends a statement to the block
   * @param stmt the statement to append
   */
  void AppendStmt(Stmt *stmt) { statements_.emplace_back(stmt); }

  /**
   * @return whether this is an empty block.
   */
  bool IsEmpty() const { return statements_.empty(); }

  /**
   * @return the last statement is the block
   */
  Stmt *LastStmt() { return (IsEmpty() ? nullptr : statements_.back()); }

  /**
   * Checks whether the given node is a BlockStmt.
   * @param node node to check
   * @return true iff given node is a BlockStmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::BlockStmt;
  }

 private:
  const SourcePosition rbrace_pos_;
  util::RegionVector<Stmt *> statements_;
};

/**
 * A statement that is just a declaration
 */
class DeclStmt : public Stmt {
 public:
  /**
   * Constructor
   * @param decl the declaration
   */
  explicit DeclStmt(Decl *decl) : Stmt(Kind::DeclStmt, decl->Position()), decl_(decl) {}

  /**
   * @return the declaration
   */
  Decl *Declaration() const { return decl_; }

  /**
   * Checks whether the given node is a DeclStmt.
   * @param node node to check
   * @return true iff given node is a DeclStmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::DeclStmt;
  }

 private:
  Decl *decl_;
};

/**
 * The bridge between statements and expressions
 */
class ExpressionStmt : public Stmt {
 public:
  /**
   * @param expr an expression
   */
  explicit ExpressionStmt(Expr *expr);

  /**
   * @return the expression
   */
  Expr *Expression() { return expr_; }

  /**
   * Checks whether the given node is an ExpressionStmt.
   * @param node node to check
   * @return true iff given node is an ExpressionStmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::ExpressionStmt;
  }

 private:
  Expr *expr_;
};

/**
 * Base class for all iteration-based statements
 */
class IterationStmt : public Stmt {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param kind kind of stmt
   * @param body loop body
   */
  IterationStmt(const SourcePosition &pos, AstNode::Kind kind, BlockStmt *body) : Stmt(kind, pos), body_(body) {}

  /**
   * @return the body of the declaration
   */
  BlockStmt *Body() const { return body_; }

  /**
   * Checks whether the given node is an IterationStmt.
   * @param node node to check
   * @return true iff given node is an IterationStmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() >= Kind::ForStmt && node->GetKind() <= Kind::ForInStmt;
  }

 private:
  BlockStmt *body_;
};

/**
 * A for statement
 */
class ForStmt : public IterationStmt {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param init loop initialization
   * @param cond loop condition
   * @param next loop update
   * @param body loop body
   */
  ForStmt(const SourcePosition &pos, Stmt *init, Expr *cond, Stmt *next, BlockStmt *body)
      : IterationStmt(pos, AstNode::Kind::ForStmt, body), init_(init), cond_(cond), next_(next) {}

  /**
   * @return the initialization stmt
   */
  Stmt *Init() const { return init_; }

  /**
   * @return the condition expr
   */
  Expr *Condition() const { return cond_; }

  /**
   * @return the update stmt
   */
  Stmt *Next() const { return next_; }

  /**
   * Checks whether the given node is an ForStmt.
   * @param node node to check
   * @return true iff given node is an ForStmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::ForStmt;
  }

 private:
  Stmt *init_;
  Expr *cond_;
  Stmt *next_;
};

/**
 * A range for statement
 */
class ForInStmt : public IterationStmt {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param target variable in which to store rows
   * @param iter container over which to iterate
   * @param body loop body
   */
  ForInStmt(const SourcePosition &pos, Expr *target, Expr *iter, BlockStmt *body)
      : IterationStmt(pos, AstNode::Kind::ForInStmt, body), target_(target), iter_(iter) {}

  /**
   * @return target variable
   */
  Expr *Target() const { return target_; }

  /**
   * @return container over which to iterate
   */
  Expr *Iter() const { return iter_; }

  /**
   * Checks whether the given node is an ForInStmt.
   * @param node node to check
   * @return true iff given node is an ForInStmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::ForInStmt;
  }

 private:
  Expr *target_;
  Expr *iter_;
};

/**
 * An if-then-else statement
 */
class IfStmt : public Stmt {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param cond if condition
   * @param then_stmt then stmt
   * @param else_stmt else stmt
   */
  IfStmt(const SourcePosition &pos, Expr *cond, BlockStmt *then_stmt, Stmt *else_stmt)
      : Stmt(Kind::IfStmt, pos), cond_(cond), then_stmt_(then_stmt), else_stmt_(else_stmt) {}

  /**
   * @return if condition
   */
  Expr *Condition() { return cond_; }

  /**
   * @return then stmt
   */
  BlockStmt *ThenStmt() { return then_stmt_; }

  /**
   * @return else stmt
   */
  Stmt *ElseStmt() { return else_stmt_; }

  /**
   * @return whether there is an else stmt
   */
  bool HasElseStmt() const { return else_stmt_ != nullptr; }

  /**
   * Checks whether the given node is an IfStmt.
   * @param node node to check
   * @return true iff given node is an IfStmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::IfStmt;
  }

 private:
  friend class sema::Sema;
  void SetCondition(Expr *cond) {
    TERRIER_ASSERT(cond != nullptr, "Cannot set null condition");
    cond_ = cond;
  }

 private:
  Expr *cond_;
  BlockStmt *then_stmt_;
  Stmt *else_stmt_;
};

/**
 * A return statement
 */
class ReturnStmt : public Stmt {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param ret expression to return
   */
  ReturnStmt(const SourcePosition &pos, Expr *ret) : Stmt(Kind::ReturnStmt, pos), ret_(ret) {}

  /**
   * @return the returned expression
   */
  Expr *Ret() { return ret_; }

  /**
   * Sets the return statement. This is used for casting.
   * @param new_ret new return statement.
   */
  void SetReturn(Expr *new_ret) { ret_ = new_ret; }

  /**
   * Checks whether the given node is an ReturnStmt.
   * @param node node to check
   * @return true iff given node is an ReturnStmt.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::ReturnStmt;
  }

 private:
  Expr *ret_;
};

// ---------------------------------------------------------
// Expression Nodes
// ---------------------------------------------------------

/**
 * Base class for all expression nodes. Expression nodes all have a required type. This type is filled in during
 * semantic analysis. Thus, GetType() will return a null pointer before type-checking.
 */
class Expr : public AstNode {
 public:
  /**
   * Value type
   */
  enum class Context : uint8_t {
    LValue,
    RValue,
    Test,
    Effect,
  };

  /**
   * Constructor
   * @param kind kind of expression
   * @param pos source position
   * @param type type of the expression
   */
  Expr(Kind kind, const SourcePosition &pos, Type *type = nullptr) : AstNode(kind, pos), type_(type) {}

  /**
   * @return the type of the expression
   */
  Type *GetType() { return type_; }

  /**
   * @return the type of the expression
   */
  const Type *GetType() const { return type_; }

  /**
   * Sets the expression type
   * @param type type of the expression
   */
  void SetType(Type *type) { type_ = type; }

  /**
   * @return Is this a nil literal?
   */
  bool IsNilLiteral() const;

  /**
   * @return Is this a string literal?
   */
  bool IsStringLiteral() const;

  /**
   * @return Is this an integer literal
   */
  bool IsIntegerLiteral() const;

  /**
   * @return Is this a boolean literal
   */
  bool IsBooleanLiteral() const;

  /**
   * Checks whether the given node is an Expr.
   * @param node node to check
   * @return true iff given node is an Expr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() >= Kind::BadExpr && node->GetKind() <= Kind::StructTypeRepr;
  }

 private:
  Type *type_;
};

/**
 * A bad expression
 */
class BadExpr : public Expr {
 public:
  /**
   * Constructor
   * @param pos source position
   */
  explicit BadExpr(const SourcePosition &pos) : Expr(AstNode::Kind::BadExpr, pos) {}

  /**
   * Checks whether the given node is an BadExpr.
   * @param node node to check
   * @return true iff given node is an BadExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::BadExpr;
  }
};

/**
 * A binary expression with non-null left and right children and an operator
 */
class BinaryOpExpr : public Expr {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param op binary operator
   * @param left lhs
   * @param right rhs
   */
  BinaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left, Expr *right)
      : Expr(Kind::BinaryOpExpr, pos), op_(op), left_(left), right_(right) {}

  /**
   * @return the binary operator
   */
  parsing::Token::Type Op() { return op_; }

  /**
   * @return the lhs
   */
  Expr *Left() { return left_; }

  /**
   * @return the rhs
   */
  Expr *Right() { return right_; }

  /**
   * Checks whether the given node is an BinaryOpExpr.
   * @param node node to check
   * @return true iff given node is an BinaryOpExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::BinaryOpExpr;
  }

 private:
  friend class sema::Sema;

  /**
   * Sets the lhs
   * @param left new lhs
   */
  void SetLeft(Expr *left) {
    TERRIER_ASSERT(left != nullptr, "Left cannot be null!");
    left_ = left;
  }

  /**
   * Sets the rhs
   * @param right new rhs
   */
  void SetRight(Expr *right) {
    TERRIER_ASSERT(right != nullptr, "Right cannot be null!");
    right_ = right;
  }

 private:
  parsing::Token::Type op_;
  Expr *left_;
  Expr *right_;
};

/**
 * A function call expression
 */
class CallExpr : public Expr {
 public:
  /**
   * Type of call (builtin call or regular function call)
   */
  enum class CallKind : uint8_t { Regular, Builtin };

  /**
   * Constructor for regular calls
   * @param func function being called
   * @param args arguments to the function
   */
  CallExpr(Expr *func, util::RegionVector<Expr *> &&args) : CallExpr(func, std::move(args), CallKind::Regular) {}

  /**
   * Constructor for arbitrary calls
   * @param func function being called
   * @param args arguments to the function
   * @param call_kind kind of call
   */
  CallExpr(Expr *func, util::RegionVector<Expr *> &&args, CallKind call_kind)
      : Expr(Kind::CallExpr, func->Position()), func_(func), args_(std::move(args)), call_kind_(call_kind) {}

  /**
   * @return the name of the function this node is calling
   */
  Identifier GetFuncName() const;

  /**
   * @return the function we're calling as an expression node
   */
  Expr *Function() { return func_; }

  /**
   * @return a reference to the arguments
   */
  const util::RegionVector<Expr *> &Arguments() const { return args_; }

  /**
   * @return the number of arguments to the function this node is calling
   */
  uint32_t NumArgs() const { return static_cast<uint32_t>(args_.size()); }

  /**
   * @return the kind of call this node represents
   */
  CallKind GetCallKind() const { return call_kind_; }

  /**
   * Checks whether the given node is an CallExpr.
   * @param node node to check
   * @return true iff given node is an CallExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::CallExpr;
  }

 private:
  friend class sema::Sema;

  /**
   * Sets the kind
   * @param call_kind new kind
   */
  void SetCallKind(CallKind call_kind) { call_kind_ = call_kind; }

  /**
   * Sets an argument
   * @param arg_idx index of the argument
   * @param expr new argument
   */
  void SetArgument(uint32_t arg_idx, Expr *expr) {
    TERRIER_ASSERT(arg_idx < NumArgs(), "Out-of-bounds argument access");
    args_[arg_idx] = expr;
  }

 private:
  Expr *func_;
  util::RegionVector<Expr *> args_;
  CallKind call_kind_;
};

/**
 * A binary comparison operator
 */
class ComparisonOpExpr : public Expr {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param op comparison operator
   * @param left lhs
   * @param right rhs
   */
  ComparisonOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left, Expr *right)
      : Expr(Kind::ComparisonOpExpr, pos), op_(op), left_(left), right_(right) {}

  /**
   * @return comparison operator
   */
  parsing::Token::Type Op() { return op_; }

  /**
   * @return the lhs
   */
  Expr *Left() { return left_; }

  /**
   * @return the rhs
   */
  Expr *Right() { return right_; }

  /**
   * Is this a comparison between an expression and a nil literal?
   * @param[out] result If this is a literal nil comparison, result will point
   *                    to the expression we're checking nil against
   * @return True if this is a nil comparison; false otherwise
   */
  bool IsLiteralCompareNil(Expr **result) const;

  /**
   * Checks whether the given node is an ComparisonOpExpr.
   * @param node node to check
   * @return true iff given node is an ComparisonOpExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::ComparisonOpExpr;
  }

 private:
  friend class sema::Sema;

  /**
   * Sets the lhs
   * @param left new lhs
   */
  void SetLeft(Expr *left) {
    TERRIER_ASSERT(left != nullptr, "Left cannot be null!");
    left_ = left;
  }

  /**
   * Sets the rhs
   * @param right new rhs
   */
  void SetRight(Expr *right) {
    TERRIER_ASSERT(right != nullptr, "Right cannot be null!");
    right_ = right;
  }

 private:
  parsing::Token::Type op_;
  Expr *left_;
  Expr *right_;
};

/**
 * A function (params, return type, body)
 */
class FunctionLitExpr : public Expr {
 public:
  /**
   * Constructor
   * @param type_repr type representation (param types, return type)
   * @param body body of the function
   */
  FunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body);

  /**
   * @return the type representation
   */
  FunctionTypeRepr *TypeRepr() const { return type_repr_; }

  /**
   * @return the body
   */
  BlockStmt *Body() const { return body_; }

  /**
   * @return whether the body is empty
   */
  bool IsEmpty() const { return Body()->IsEmpty(); }

  /**
   * Checks whether the given node is an FunctionLitExpr.
   * @param node node to check
   * @return true iff given node is an FunctionLitExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::FunctionLitExpr;
  }

 private:
  FunctionTypeRepr *type_repr_;
  BlockStmt *body_;
};

/**
 * A reference to a variable, function or struct
 */
class IdentifierExpr : public Expr {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param name identifier of the variable, function or struct
   */
  IdentifierExpr(const SourcePosition &pos, Identifier name)
      : Expr(Kind::IdentifierExpr, pos), name_(name), decl_(nullptr) {}

  /**
   * @return the identifier
   */
  Identifier Name() const { return name_; }

  /**
   * Binds a declaration
   * @param decl the declaration to bind.
   */
  void BindTo(Decl *decl) { decl_ = decl; }

  /**
   * @return the identifier is bound
   */
  bool IsBound() const { return decl_ != nullptr; }

  /**
   * Checks whether the given node is an IdentifierExpr.
   * @param node node to check
   * @return true iff given node is an IdentifierExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::IdentifierExpr;
  }

 private:
  // TODO(pmenon) Should these two be a union since only one should be active?
  // Pre-binding, 'name_' is used, and post-binding 'decl_' should be used?
  Identifier name_;
  Decl *decl_;
};

/**
 * An enumeration capturing all possible casting operations
 */
enum class CastKind : uint8_t {
  // Conversion of a 32-bit integer into a non-nullable SQL Integer value
  IntToSqlInt,

  // Conversion of a 32-bit integer into a non-nullable SQL Decimal value
  IntToSqlDecimal,

  // Conversion of a SQL boolean value (potentially nullable) into a primitive
  // boolean value
  SqlBoolToBool,

  // Conversion of a primitive boolean into a SQL boolean
  BoolToSqlBool,

  // A cast between integral types (i.e., 8-bit, 16-bit, 32-bit, or 64-bit
  // numbers), excluding to boolean! Boils down to a bitcast, a truncation,
  // a sign-extension, or a zero-extension. The same as in C/C++.
  IntegralCast,

  // An integer to float cast. Only allows widening.
  IntToFloat,

  // A float to integer cast. Only allows widening.
  FloatToInt,

  // A simple bit cast reinterpretation
  BitCast,

  // 64 bit float To Sql Real
  FloatToSqlReal,

  // Conversion of a SQL timestamp value (potentially nullable) into a primitive timestamp value
  SqlTimestampToTimestamp,

  // Conversion of a primitive timestamp valueinto a SQL timestamp
  TimestampToSqlTimestamp,

  // Convert a SQL integer into a SQL real
  SqlIntToSqlReal,
};

/**
 * An implicit cast operation is one that is inserted automatically by the compiler during semantic analysis.
 */
class ImplicitCastExpr : public Expr {
 public:
  /**
   * @return kind of the cast
   */
  CastKind GetCastKind() const { return cast_kind_; }

  /**
   * @return input of the cast
   */
  Expr *Input() { return input_; }

  /**
   * Checks whether the given node is an ImplicitCastExpr.
   * @param node node to check
   * @return true iff given node is an ImplicitCastExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::ImplicitCastExpr;
  }

 private:
  friend class AstNodeFactory;

  /**
   * Constructor
   * @param pos source position
   * @param cast_kind kind of the cast
   * @param target_type type of the resulting expression
   * @param input input of the cast
   */
  ImplicitCastExpr(const SourcePosition &pos, CastKind cast_kind, Type *target_type, Expr *input)
      : Expr(Kind::ImplicitCastExpr, pos, target_type), cast_kind_(cast_kind), input_(input) {}

 private:
  CastKind cast_kind_;
  Expr *input_;
};

/**
 * Expressions for array or map accesses, e.g., x[i]. The object ('x' in the example) can either be an array or a map.
 * The index ('i' in the example) must evaluate to an integer for array access and the map's associated key type if the
 * object is a map.
 */
class IndexExpr : public Expr {
 public:
  /**
   * @return the object being indexed
   */
  Expr *Object() const { return obj_; }

  /**
   * @return the index
   */
  Expr *Index() const { return index_; }

  /**
   * Is this expression for an array access?
   * @return True if for array; false otherwise
   */
  bool IsArrayAccess() const;

  /**
   * Is this expression for a map access?
   * @return True if for a map; false otherwise
   */
  bool IsMapAccess() const;

  /**
   * Checks whether the given node is an IndexExpr.
   * @param node node to check
   * @return true iff given node is an IndexExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::IndexExpr;
  }

 private:
  friend class AstNodeFactory;

  /**
   * Constructor
   * @param pos source position
   * @param obj object being indexed
   * @param index index to retrieve
   */
  IndexExpr(const SourcePosition &pos, Expr *obj, Expr *index) : Expr(Kind::IndexExpr, pos), obj_(obj), index_(index) {}

 private:
  Expr *obj_;
  Expr *index_;
};

/**
 * A literal in the original source code
 */
class LitExpr : public Expr {
 public:
  /**
   * Enum of kinds of literal expressions.
   */
  enum class LitKind : uint8_t { Nil, Boolean, Int, Float, String };

  /**
   * Nil constructor
   * @param pos source position
   */
  explicit LitExpr(const SourcePosition &pos) : Expr(Kind::LitExpr, pos), lit_kind_(LitExpr::LitKind::Nil) {}

  /**
   * Bool constructor
   * @param pos source position
   * @param val boolean value
   */
  LitExpr(const SourcePosition &pos, bool val)
      : Expr(Kind::LitExpr, pos), lit_kind_(LitExpr::LitKind::Boolean), boolean_(val) {}

  /**
   * String constructor
   * @param pos source possition
   * @param str string value
   */
  LitExpr(const SourcePosition &pos, Identifier str)
      : Expr(Kind::LitExpr, pos), lit_kind_(LitKind::String), str_(str) {}

  /**
   * Integer constructor
   * @param pos source position
   * @param num integer value
   */
  LitExpr(const SourcePosition &pos, int64_t num) : Expr(Kind::LitExpr, pos), lit_kind_(LitKind::Int), int64_(num) {}

  /**
   * Float constructor
   * @param pos source position
   * @param num float value
   */
  LitExpr(const SourcePosition &pos, double num) : Expr(Kind::LitExpr, pos), lit_kind_(LitKind::Float), float64_(num) {}

  /**
   * @return the literal kind
   */
  LitExpr::LitKind LiteralKind() const { return lit_kind_; }

  /**
   * @return Is this a nil literal?
   */
  bool IsNilLitExpr() const { return lit_kind_ == LitKind::Nil; }

  /**
   * @return Is this a bool literal?
   */
  bool IsBoolLitExpr() const { return lit_kind_ == LitKind::Boolean; }

  /**
   * @return Is this an int literal?
   */
  bool IsIntLitExpr() const { return lit_kind_ == LitKind::Int; }

  /**
   * @return Is this a float literal?
   */
  bool IsFloatLitExpr() const { return lit_kind_ == LitKind::Float; }

  /**
   * @return Is this a string literal?
   */
  bool IsStringLitExpr() const { return lit_kind_ == LitKind::String; }

  /**
   * @return the boolean value
   */
  bool BoolVal() const {
    TERRIER_ASSERT(LiteralKind() == LitKind::Boolean, "Getting boolean value from a non-bool expression!");
    return boolean_;
  }

  /**
   * @return the string value
   */
  Identifier RawStringVal() const {
    TERRIER_ASSERT(LiteralKind() != LitKind::Nil && LiteralKind() != LitKind::Boolean,
                   "Getting a raw string value from a non-string or numeric value");
    return str_;
  }

  /**
   * @return the integer value
   */
  int64_t Int64Val() const {
    TERRIER_ASSERT(LiteralKind() == LitKind::Int, "Getting integer value from a non-integer literal expression");
    return int64_;
  }

  /**
   * @return the float value
   */
  double Float64Val() const {
    TERRIER_ASSERT(LiteralKind() == LitKind::Float, "Getting float value from a non-float literal expression");
    return float64_;
  }

  /**
   * Checks whether the given node is an LitExpr.
   * @param node node to check
   * @return true iff given node is an LitExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::LitExpr;
  }

 private:
  LitKind lit_kind_;

  /**
   * Union of possible literal values
   */
  union {
    bool boolean_;
    Identifier str_;
    byte *bytes_;
    int64_t int64_;
    double float64_;
  };
};

/**
 * Expressions accessing structure members, e.g., x.f
 *
 * TPL uses the same member access syntax for regular struct member access and
 * access through a struct pointer. Thus, the language allows the following:
 *
 * @code
 * struct X {
 *   a: int
 * }
 *
 * var x: X
 * var px: *X
 *
 * x.a = 10
 * px.a = 20
 * @endcode
 *
 */
class MemberExpr : public Expr {
 public:
  /**
   * @return object being accessed
   */
  Expr *Object() const { return object_; }

  /**
   * @return member being accessed
   */
  Expr *Member() const { return member_; }

  /**
   * @return Whether a struct pointer is being accesed
   */
  bool IsSugaredArrow() const;

  /**
   * Checks whether the given node is an MemberExpr.
   * @param node node to check
   * @return true iff given node is an MemberExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::MemberExpr;
  }

 private:
  friend class AstNodeFactory;

  /**
   * Construct
   * @param pos source position
   * @param obj object being accessed
   * @param member member being accessed
   */
  MemberExpr(const SourcePosition &pos, Expr *obj, Expr *member)
      : Expr(Kind::MemberExpr, pos), object_(obj), member_(member) {}

 private:
  Expr *object_;
  Expr *member_;
};

/**
 * A unary expression with a non-null inner expression and an operator
 */
class UnaryOpExpr : public Expr {
 public:
  /**
   * Constuctor
   * @param pos source position
   * @param op unary operator
   * @param expr operand
   */
  UnaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *expr)
      : Expr(Kind::UnaryOpExpr, pos), op_(op), expr_(expr) {}

  /**
   * @return the unary operator
   */
  parsing::Token::Type Op() { return op_; }

  /**
   * @return the operand
   */
  Expr *Expression() { return expr_; }

  /**
   * Checks whether the given node is an UnaryOpExpr.
   * @param node node to check
   * @return true iff given node is an UnaryOpExpr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::UnaryOpExpr;
  }

 private:
  parsing::Token::Type op_;
  Expr *expr_;
};

// ---------------------------------------------------------
// Type Representation Nodes
// ---------------------------------------------------------

//
// Type representation nodes. A type representation is a thin representation of
// how the type appears in code. They are structurally the same as their full
// blown Type counterparts, but we use the expressions to defer their type
// resolution.
//

/**
 * Array type
 */
class ArrayTypeRepr : public Expr {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param len length of the array
   * @param elem_type element type
   */
  ArrayTypeRepr(const SourcePosition &pos, Expr *len, Expr *elem_type)
      : Expr(Kind::ArrayTypeRepr, pos), len_(len), elem_type_(elem_type) {}

  /**
   * @return the length of the array
   */
  Expr *Length() const { return len_; }

  /**
   * @return the element type
   */
  Expr *ElementType() const { return elem_type_; }

  /**
   * @return whether the length is given
   */
  bool HasLength() const { return len_ != nullptr; }

  /**
   * Checks whether the given node is an ArrayTypeRepr.
   * @param node node to check
   * @return true iff given node is an ArrayTypeRepr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::ArrayTypeRepr;
  }

 private:
  Expr *len_;
  Expr *elem_type_;
};

/**
 * Function type (params and return type)
 */
class FunctionTypeRepr : public Expr {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param param_types param types
   * @param ret_type return type
   */
  FunctionTypeRepr(const SourcePosition &pos, util::RegionVector<FieldDecl *> &&param_types, Expr *ret_type)
      : Expr(Kind::FunctionTypeRepr, pos), param_types_(std::move(param_types)), ret_type_(ret_type) {}

  /**
   * @return list of param types
   */
  const util::RegionVector<FieldDecl *> &Paramaters() const { return param_types_; }

  /**
   * @return the return type
   */
  Expr *ReturnType() const { return ret_type_; }

  /**
   * Checks whether the given node is an FunctionTypeRepr.
   * @param node node to check
   * @return true iff given node is an FunctionTypeRepr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::FunctionTypeRepr;
  }

 private:
  util::RegionVector<FieldDecl *> param_types_;
  Expr *ret_type_;
};

/**
 * Map types
 */
class MapTypeRepr : public Expr {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param key key tyoe
   * @param val value type
   */
  MapTypeRepr(const SourcePosition &pos, Expr *key, Expr *val) : Expr(Kind::MapTypeRepr, pos), key_(key), val_(val) {}

  /**
   * @return the key type
   */
  Expr *Key() const { return key_; }

  /**
   * @return the value type
   */
  Expr *Val() const { return val_; }

  /**
   * Checks whether the given node is an MapTypeRepr.
   * @param node node to check
   * @return true iff given node is an MapTypeRepr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::MapTypeRepr;
  }

 private:
  Expr *key_;
  Expr *val_;
};

/**
 * Pointer type
 */
class PointerTypeRepr : public Expr {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param base pointee type
   */
  PointerTypeRepr(const SourcePosition &pos, Expr *base) : Expr(Kind::PointerTypeRepr, pos), base_(base) {}

  /**
   * @return the pointee type
   */
  Expr *Base() const { return base_; }

  /**
   * Checks whether the given node is an PointerTypeRepr.
   * @param node node to check
   * @return true iff given node is an PointerTypeRepr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::PointerTypeRepr;
  }

 private:
  Expr *base_;
};

/**
 * Struct type
 */
class StructTypeRepr : public Expr {
 public:
  /**
   * Constructor
   * @param pos source position
   * @param fields list of fields
   */
  StructTypeRepr(const SourcePosition &pos, util::RegionVector<FieldDecl *> &&fields)
      : Expr(Kind::StructTypeRepr, pos), fields_(std::move(fields)) {}

  /**
   * @return the list of fields
   */
  const util::RegionVector<FieldDecl *> &Fields() const { return fields_; }

  /**
   * Checks whether the given node is an StructTypeRepr.
   * @param node node to check
   * @return true iff given node is an StructTypeRepr.
   */
  static bool classof(const AstNode *node) {  // NOLINT
    return node->GetKind() == Kind::StructTypeRepr;
  }

 private:
  util::RegionVector<FieldDecl *> fields_;
};

}  // namespace ast
}  // namespace terrier::execution
