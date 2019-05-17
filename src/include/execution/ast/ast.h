#pragma once

#include <cstdint>
#include <utility>

#include "llvm/Support/Casting.h"

#include "execution/ast/identifier.h"
#include "execution/parsing/token.h"
#include "execution/compiler/compiler_defs.h"
#include "execution/util/common.h"
#include "execution/util/region.h"
#include "execution/util/region_containers.h"

namespace tpl {

namespace sema {
class Sema;
}  // namespace sema

namespace ast {

/// Top-level file node
#define FILE_NODE(T) T(File)

/// All possible declaration types.
/// NOTE: If you add a new declaration node to either the beginning or end of
/// the list, remember to modify Decl::classof() to update the bounds check.
#define DECLARATION_NODES(T) \
  T(FieldDecl)               \
  T(FunctionDecl)            \
  T(StructDecl)              \
  T(VariableDecl)

/// All possible statements
/// NOTE: If you add a new statement node to either the beginning or end of the
/// list, remember to modify Stmt::classof() to update the bounds check.
#define STATEMENT_NODES(T) \
  T(AssignmentStmt)        \
  T(BlockStmt)             \
  T(DeclStmt)              \
  T(ExpressionStmt)        \
  T(ForStmt)               \
  T(ForInStmt)             \
  T(IfStmt)                \
  T(ReturnStmt)

/// All possible expressions
/// NOTE: If you add a new expression node to either the beginning or end of the
/// list, remember to modify Expr::classof() to update the bounds check.
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

/// All AST nodes
#define AST_NODES(T)   \
  DECLARATION_NODES(T) \
  EXPRESSION_NODES(T)  \
  FILE_NODE(T)         \
  STATEMENT_NODES(T)

// Forward declare some base classes
class Decl;
class Expr;
class Stmt;
class Type;

// Forward declare all nodes
#define FORWARD_DECLARE(name) class name;
AST_NODES(FORWARD_DECLARE)
#undef FORWARD_DECLARE

// ---------------------------------------------------------
// AST Node
// ---------------------------------------------------------

/// The base class for all AST nodes. AST nodes are emphemeral, and thus, are
/// only allocated from regions. Once created, they are immutable only during
/// semantic checks - this is why you'll often see sema::Sema declared as a
/// friend class in some concrete node subclasses.
///
/// All nodes have a "kind" that represents as an ID indicating the specific
/// kind of AST node it is (i.e., if it's an if-statement or a binary
/// expression). You can query the node for it's kind, but it's usually more
/// informative and clear to use the Is() method.
class AstNode : public util::RegionObject {
 public:
  // The kind enumeration listing all possible node kinds
#define T(kind) kind,
  enum class Kind : u8 { AST_NODES(T) };
#undef T

  /// Return the kind of this node
  Kind kind() const { return kind_; }

  /// Retrn the position in the source where this element was found
  const SourcePosition &position() const { return pos_; }

  /// Return the name of this node. NOTE: this is mainly used in tests!
  const char *kind_name() const {
#define KIND_CASE(kind) \
  case Kind::kind:      \
    return #kind;

    // Main type switch
    // clang-format off
    switch (kind()) {
      default: { UNREACHABLE("Impossible kind name"); }
      AST_NODES(KIND_CASE)
    }
      // clang-format on
#undef KIND_CASE
  }

  // Checks if this node is an instance of the specified class
  template <typename T>
  bool Is() const {
    return llvm::isa<T>(this);
  }

  // Casts this node to an instance of the specified class, asserting if the
  // conversion is invalid. This is probably most similar to std::static_cast<>
  // or std::reinterpret_cast<>
  template <typename T>
  T *As() {
    TPL_ASSERT(Is<T>(), "Using unsafe cast on mismatched node types");
    return reinterpret_cast<T *>(this);
  }

  template <typename T>
  const T *As() const {
    TPL_ASSERT(Is<T>(), "Using unsafe cast on mismatched node types");
    return reinterpret_cast<const T *>(this);
  }

  // Casts this node to an instance of the provided class if valid. If the
  // conversion is invalid, this returns a NULL pointer. This is most similar to
  // std::dynamic_cast<T>, i.e., it's a checked cast.
  template <typename T>
  T *SafeAs() {
    return (Is<T>() ? As<T>() : nullptr);
  }

  template <typename T>
  const T *SafeAs() const {
    return (Is<T>() ? As<T>() : nullptr);
  }

#define F(kind) \
  bool Is##kind() const { return Is<kind>(); }
  AST_NODES(F)
#undef F

 protected:
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
  File(const SourcePosition &pos, util::RegionVector<Decl *> &&decls)
      : AstNode(Kind::File, pos), decls_(std::move(decls)) {}

  util::RegionVector<Decl *> &declarations() { return decls_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::File;
  }

 private:
  util::RegionVector<Decl *> decls_;
};

// ---------------------------------------------------------
// Declaration Nodes
// ---------------------------------------------------------

/// Base class for all declarations in TPL. All declarations have a name, and
/// an optional type representation. Structure and function declarations have an
/// explicit type, but variables may not.
class Decl : public AstNode {
 public:
  Decl(Kind kind, const SourcePosition &pos, Identifier name, Expr *type_repr)
      : AstNode(kind, pos), name_(name), type_repr_(type_repr) {}

  Identifier name() const { return name_; }

  Expr *type_repr() const { return type_repr_; }

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::FieldDecl &&
           node->kind() <= Kind::VariableDecl;
  }

 private:
  Identifier name_;
  Expr *type_repr_;
};

/// A generic declaration of a function argument or a field in a struct
class FieldDecl : public Decl {
 public:
  FieldDecl(const SourcePosition &pos, Identifier name, Expr *type_repr)
      : Decl(Kind::FieldDecl, pos, name, type_repr) {}

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FieldDecl;
  }
};

/// A function declaration
class FunctionDecl : public Decl {
 public:
  FunctionDecl(const SourcePosition &pos, Identifier name,
               FunctionLitExpr *func);

  FunctionLitExpr *function() const { return func_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionDecl;
  }

 private:
  FunctionLitExpr *func_;
};

/// A structure declaration
class StructDecl : public Decl {
 public:
  StructDecl(const SourcePosition &pos, Identifier name,
             StructTypeRepr *type_repr);

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::StructDecl;
  }
};

/// A variable declaration
class VariableDecl : public Decl {
 public:
  VariableDecl(const SourcePosition &pos, Identifier name, Expr *type_repr,
               Expr *init)
      : Decl(Kind::VariableDecl, pos, name, type_repr), init_(init) {}

  Expr *initial() const { return init_; }

  // Did the variable declaration come with an explicit type i.e., var x:int = 0
  bool HasTypeDecl() const { return type_repr() != nullptr; }

  bool HasInitialValue() const { return init_ != nullptr; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::VariableDecl;
  }

 private:
  Expr *init_;
};

// ---------------------------------------------------------
// Statement Nodes
// ---------------------------------------------------------

/// Base class for all statement nodes
class Stmt : public AstNode {
 public:
  Stmt(Kind kind, const SourcePosition &pos) : AstNode(kind, pos) {}

  /// Determines if \a stmt, the last in a statement list, is terminating
  /// \param stmt The statement node to check
  /// \return True if statement has a terminator; false otherwise
  static bool IsTerminating(Stmt *stmt);

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::AssignmentStmt &&
           node->kind() <= Kind::ReturnStmt;
  }
};

/// An assignment, dest = source
class AssignmentStmt : public Stmt {
 public:
  AssignmentStmt(const SourcePosition &pos, Expr *dest, Expr *src)
      : Stmt(AstNode::Kind::AssignmentStmt, pos), dest_(dest), src_(src) {}

  Expr *destination() const { return dest_; }

  Expr *source() const { return src_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::AssignmentStmt;
  }

 private:
  friend class sema::Sema;

  // Used for implicit casts
  void set_source(Expr *source) { src_ = source; }

 private:
  Expr *dest_;
  Expr *src_;
};

/// A block of statements
class BlockStmt : public Stmt {
 public:
  BlockStmt(const SourcePosition &pos, const SourcePosition &rbrace_pos,
            util::RegionVector<Stmt *> &&statements)
      : Stmt(Kind::BlockStmt, pos),
        rbrace_pos_(rbrace_pos),
        statements_(std::move(statements)) {}

  util::RegionVector<Stmt *> &statements() { return statements_; }

  const SourcePosition &right_brace_position() const { return rbrace_pos_; }

  void AppendStmt(Stmt *stmt) {
    statements_.emplace_back(stmt);
  }

  bool IsEmpty() const { return statements_.empty(); }

  Stmt *LastStmt() { return (IsEmpty() ? nullptr : statements_.back()); }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BlockStmt;
  }

 private:
  const SourcePosition rbrace_pos_;
  util::RegionVector<Stmt *> statements_;
};

/// A statement that is just a declaration
class DeclStmt : public Stmt {
 public:
  explicit DeclStmt(Decl *decl)
      : Stmt(Kind::DeclStmt, decl->position()), decl_(decl) {}

  Decl *declaration() const { return decl_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::DeclStmt;
  }

 private:
  Decl *decl_;
};

/// The bridge between statements and expressions
class ExpressionStmt : public Stmt {
 public:
  explicit ExpressionStmt(Expr *expr);

  Expr *expression() { return expr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ExpressionStmt;
  }

 private:
  Expr *expr_;
};

/// Base class for all iteration-based statements
class IterationStmt : public Stmt {
 public:
  IterationStmt(const SourcePosition &pos, AstNode::Kind kind, BlockStmt *body)
      : Stmt(kind, pos), body_(body) {}

  BlockStmt *body() const { return body_; }

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::ForStmt && node->kind() <= Kind::ForInStmt;
  }

 private:
  BlockStmt *body_;
};

/// A for statement
class ForStmt : public IterationStmt {
 public:
  ForStmt(const SourcePosition &pos, Stmt *init, Expr *cond, Stmt *next,
          BlockStmt *body)
      : IterationStmt(pos, AstNode::Kind::ForStmt, body),
        init_(init),
        cond_(cond),
        next_(next) {}

  Stmt *init() const { return init_; }

  Expr *condition() const { return cond_; }

  Stmt *next() const { return next_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ForStmt;
  }

 private:
  Stmt *init_;
  Expr *cond_;
  Stmt *next_;
};

/// Generic grab bag of key-value attributes that can be associated with any AST
/// nodes in the tree.
class Attributes : public util::RegionObject {
 public:
  explicit Attributes(util::RegionUnorderedMap<Identifier, Expr *> &&map)
      : map_(std::move(map)) {}

  Expr *Find(Identifier identifier) const {
    if (const auto iter = map_.find(identifier); iter != map_.end()) {
      return iter->second;
    }

    return nullptr;
  }

  bool Contains(Identifier identifier) const {
    return Find(identifier) != nullptr;
  }

 private:
  util::RegionUnorderedMap<Identifier, Expr *> map_;
};

/// A range for statement
class ForInStmt : public IterationStmt {
 public:
  ForInStmt(const SourcePosition &pos, Expr *target, Expr *iter,
            Attributes *attributes, BlockStmt *body)
      : IterationStmt(pos, AstNode::Kind::ForInStmt, body),
        target_(target),
        iter_(iter),
        attributes_(attributes) {}

  Expr *target() const { return target_; }

  Expr *iter() const { return iter_; }

  Attributes *attributes() const { return attributes_; }

  bool GetHasOid() const { return attributes() != nullptr
        && attributes()->Contains(ast::Identifier(OID_KEY)); }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ForInStmt;
  }

 private:
  Expr *target_;
  Expr *iter_;
  Attributes *attributes_;
};

/// An if-then-else statement
class IfStmt : public Stmt {
 public:
  IfStmt(const SourcePosition &pos, Expr *cond, BlockStmt *then_stmt,
         Stmt *else_stmt)
      : Stmt(Kind::IfStmt, pos),
        cond_(cond),
        then_stmt_(then_stmt),
        else_stmt_(else_stmt) {}

  Expr *condition() { return cond_; }

  BlockStmt *then_stmt() { return then_stmt_; }

  Stmt *else_stmt() { return else_stmt_; }

  bool HasElseStmt() const { return else_stmt_ != nullptr; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::IfStmt;
  }

 private:
  friend class sema::Sema;
  void set_condition(Expr *cond) {
    TPL_ASSERT(cond != nullptr, "Cannot set null condition");
    cond_ = cond;
  }

 private:
  Expr *cond_;
  BlockStmt *then_stmt_;
  Stmt *else_stmt_;
};

/// A return statement
class ReturnStmt : public Stmt {
 public:
  ReturnStmt(const SourcePosition &pos, Expr *ret)
      : Stmt(Kind::ReturnStmt, pos), ret_(ret) {}

  Expr *ret() { return ret_; }

  bool HasExpressionValue() const { return ret_ != nullptr; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ReturnStmt;
  }

 private:
  Expr *ret_;
};

// ---------------------------------------------------------
// Expression Nodes
// ---------------------------------------------------------

/// Base class for all expression nodes. Expression nodes all have a required
/// type. This type is filled in during semantic analysis. Thus, type() will
/// return a null pointer before type-checking.
class Expr : public AstNode {
 public:
  enum class Context : u8 {
    LValue,
    RValue,
    Test,
    Effect,
  };

  Expr(Kind kind, const SourcePosition &pos, Type *type = nullptr)
      : AstNode(kind, pos), type_(type) {}

  Type *type() { return type_; }
  const Type *type() const { return type_; }

  void set_type(Type *type) { type_ = type; }

  static bool classof(const AstNode *node) {
    return node->kind() >= Kind::BadExpr &&
           node->kind() <= Kind::StructTypeRepr;
  }

 private:
  Type *type_;
};

/// A bad statement
class BadExpr : public Expr {
 public:
  explicit BadExpr(const SourcePosition &pos)
      : Expr(AstNode::Kind::BadExpr, pos) {}

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BadExpr;
  }
};

/// A binary expression with non-null left and right children and an operator
class BinaryOpExpr : public Expr {
 public:
  BinaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *left,
               Expr *right)
      : Expr(Kind::BinaryOpExpr, pos), op_(op), left_(left), right_(right) {}

  parsing::Token::Type op() { return op_; }

  Expr *left() { return left_; }

  Expr *right() { return right_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::BinaryOpExpr;
  }

 private:
  friend class sema::Sema;

  void set_left(Expr *left) {
    TPL_ASSERT(left != nullptr, "Left cannot be null!");
    left_ = left;
  }

  void set_right(Expr *right) {
    TPL_ASSERT(right != nullptr, "Right cannot be null!");
    right_ = right;
  }

 private:
  parsing::Token::Type op_;
  Expr *left_;
  Expr *right_;
};

/// A function call expression
class CallExpr : public Expr {
 public:
  enum class CallKind : u8 { Regular, Builtin };

  CallExpr(Expr *func, util::RegionVector<Expr *> &&args)
      : Expr(Kind::CallExpr, func->position()),
        func_(func),
        args_(std::move(args)),
        call_kind_(CallKind::Regular) {}

  /// Return the name of the function this node is calling
  Identifier GetFuncName() const;

  /// Return the function we're calling as an expression node
  Expr *function() { return func_; }

  /// Return a reference to the arguments
  const util::RegionVector<Expr *> &arguments() const { return args_; }

  /// Return the number of arguments to the function this node is calling
  u32 num_args() const { return static_cast<u32>(args_.size()); }

  /// Return the kind of call this node represents
  CallKind call_kind() const { return call_kind_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::CallExpr;
  }

 private:
  friend class sema::Sema;

  void set_call_kind(CallKind call_kind) { call_kind_ = call_kind; }

  void set_argument(u32 arg_idx, Expr *expr) {
    TPL_ASSERT(arg_idx < num_args(), "Out-of-bounds argument access");
    args_[arg_idx] = expr;
  }

 private:
  Expr *func_;
  util::RegionVector<Expr *> args_;
  CallKind call_kind_;
};

/// A binary comparison operator
class ComparisonOpExpr : public Expr {
 public:
  ComparisonOpExpr(const SourcePosition &pos, parsing::Token::Type op,
                   Expr *left, Expr *right)
      : Expr(Kind::ComparisonOpExpr, pos),
        op_(op),
        left_(left),
        right_(right) {}

  parsing::Token::Type op() { return op_; }

  Expr *left() { return left_; }

  Expr *right() { return right_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ComparisonOpExpr;
  }

 private:
  friend class sema::Sema;

  void set_left(Expr *left) {
    TPL_ASSERT(left != nullptr, "Left cannot be null!");
    left_ = left;
  }

  void set_right(Expr *right) {
    TPL_ASSERT(right != nullptr, "Right cannot be null!");
    right_ = right;
  }

 private:
  parsing::Token::Type op_;
  Expr *left_;
  Expr *right_;
};

/// A function
class FunctionLitExpr : public Expr {
 public:
  FunctionLitExpr(FunctionTypeRepr *type_repr, BlockStmt *body);

  FunctionTypeRepr *type_repr() const { return type_repr_; }

  BlockStmt *body() const { return body_; }

  bool IsEmpty() const { return body()->IsEmpty(); }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionLitExpr;
  }

 private:
  FunctionTypeRepr *type_repr_;
  BlockStmt *body_;
};

/// A reference to a variable, function or struct
class IdentifierExpr : public Expr {
 public:
  IdentifierExpr(const SourcePosition &pos, Identifier name)
      : Expr(Kind::IdentifierExpr, pos), name_(name), decl_(nullptr) {}

  Identifier name() const { return name_; }

  void BindTo(Decl *decl) { decl_ = decl; }

  bool is_bound() const { return decl_ != nullptr; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::IdentifierExpr;
  }

 private:
  // TODO(pmenon) Should these two be a union since only one should be active?
  // Pre-binding, 'name_' is used, and post-binding 'decl_' should be used?
  Identifier name_;
  Decl *decl_;
};

/// An enumeration capturing all possible casting operations
enum class CastKind : u8 {
  // Conversion of a 32-bit integer into a non-nullable SQL Integer value
  IntToSqlInt,

  // Conversion of a 32-bit integer into a non-nullable SQL Decimal value
  IntToSqlDecimal,

  // Conversion of a SQL boolean value (potentially nullable) into a primitive
  // boolean value
  SqlBoolToBool,

  // A cast between integral types (i.e., 8-bit, 16-bit, 32-bit, or 64-bit
  // numbers), excluding to boolean! Boils down to a bitcast, a truncation,
  // a sign-extension, or a zero-extension. The same as in C/C++.
  IntegralCast,
};

/// An implicit cast operation is one that is inserted automatically by the
/// compiler during semantic analysis.
class ImplicitCastExpr : public Expr {
 public:
  CastKind cast_kind() const { return cast_kind_; }

  Expr *input() { return input_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ImplicitCastExpr;
  }

 private:
  friend class AstNodeFactory;

  ImplicitCastExpr(const SourcePosition &pos, CastKind cast_kind,
                   Type *target_type, Expr *input)
      : Expr(Kind::ImplicitCastExpr, pos, target_type),
        cast_kind_(cast_kind),
        input_(input) {}

 private:
  CastKind cast_kind_;
  Expr *input_;
};

/// Expressions for array or map accesses, e.g., x[i]. The object ('x' in the
/// example) can either be an array or a map. The index ('i' in the example)
/// must evaluate to an integer for array access and the map's associated key
/// type if the object is a map.
class IndexExpr : public Expr {
 public:
  Expr *object() const { return obj_; }

  Expr *index() const { return index_; }

  /// Is this expression for an array access?
  /// \return True if for array; false otherwise
  bool IsArrayAccess() const;

  /// Is this expression for a map access?
  /// \return True if for a map; false otherwise
  bool IsMapAccess() const;

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::IndexExpr;
  }

 private:
  friend class AstNodeFactory;

  IndexExpr(const SourcePosition &pos, Expr *obj, Expr *index)
      : Expr(Kind::IndexExpr, pos), obj_(obj), index_(index) {}

 private:
  Expr *obj_;
  Expr *index_;
};

/// A literal in the original source code
class LitExpr : public Expr {
 public:
  enum class LitKind : u8 { Nil, Boolean, Int, Float, String };

  explicit LitExpr(const SourcePosition &pos)
      : Expr(Kind::LitExpr, pos), lit_kind_(LitExpr::LitKind::Nil) {}

  LitExpr(const SourcePosition &pos, bool val)
      : Expr(Kind::LitExpr, pos),
        lit_kind_(LitExpr::LitKind::Boolean),
        boolean_(val) {}

  LitExpr(const SourcePosition &pos, LitExpr::LitKind lit_kind, Identifier str)
      : Expr(Kind::LitExpr, pos), lit_kind_(lit_kind), str_(str) {}

  LitExpr(const SourcePosition &pos, i32 num)
      : Expr(Kind::LitExpr, pos), lit_kind_(LitKind::Int), int32_(num) {}

  LitExpr(const SourcePosition &pos, f32 num)
      : Expr(Kind::LitExpr, pos), lit_kind_(LitKind::Float), float32_(num) {}

  LitExpr::LitKind literal_kind() const { return lit_kind_; }

  bool bool_val() const {
    TPL_ASSERT(literal_kind() == LitKind::Boolean,
               "Getting boolean value from a non-bool expression!");
    return boolean_;
  }

  Identifier raw_string_val() const {
    TPL_ASSERT(
        literal_kind() != LitKind::Nil && literal_kind() != LitKind::Boolean,
        "Getting a raw string value from a non-string or numeric value");
    return str_;
  }

  i32 int32_val() const {
    TPL_ASSERT(literal_kind() == LitKind::Int,
               "Getting integer value from a non-integer literal expression");
    return int32_;
  }

  f32 float32_val() const {
    TPL_ASSERT(literal_kind() == LitKind::Float,
               "Getting float value from a non-float literal expression");
    return float32_;
  }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::LitExpr;
  }

 private:
  LitKind lit_kind_;

  union {
    bool boolean_;
    Identifier str_;
    i32 int32_;
    f32 float32_;
  };
};

/// Expressions accessing structure members, e.g., x.f
///
/// TPL uses the same member access syntax for regular struct member access and
/// access through a struct pointer. Thus, the language allows the following:
///
/// \code
/// struct X {
///   a: int
/// }
///
/// var x: X
/// var px: *X
///
/// x.a = 10
/// px.a = 20
/// \endcode
class MemberExpr : public Expr {
 public:
  Expr *object() const { return object_; }

  Expr *member() const { return member_; }

  bool IsSugaredArrow() const;

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::MemberExpr;
  }

 private:
  friend class AstNodeFactory;

  MemberExpr(const SourcePosition &pos, Expr *obj, Expr *member)
      : Expr(Kind::MemberExpr, pos), object_(obj), member_(member) {}

 private:
  Expr *object_;
  Expr *member_;
};

/// A unary expression with a non-null inner expression and an operator
class UnaryOpExpr : public Expr {
 public:
  UnaryOpExpr(const SourcePosition &pos, parsing::Token::Type op, Expr *expr)
      : Expr(Kind::UnaryOpExpr, pos), op_(op), expr_(expr) {}

  parsing::Token::Type op() { return op_; }

  Expr *expr() { return expr_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::UnaryOpExpr;
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

/// Array type
class ArrayTypeRepr : public Expr {
 public:
  ArrayTypeRepr(const SourcePosition &pos, Expr *len, Expr *elem_type)
      : Expr(Kind::ArrayTypeRepr, pos), len_(len), elem_type_(elem_type) {}

  Expr *length() const { return len_; }

  Expr *element_type() const { return elem_type_; }

  bool HasLength() const { return len_ != nullptr; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::ArrayTypeRepr;
  }

 private:
  Expr *len_;
  Expr *elem_type_;
};

/// Function type
class FunctionTypeRepr : public Expr {
 public:
  FunctionTypeRepr(const SourcePosition &pos,
                   util::RegionVector<FieldDecl *> &&param_types,
                   Expr *ret_type)
      : Expr(Kind::FunctionTypeRepr, pos),
        param_types_(std::move(param_types)),
        ret_type_(ret_type) {}

  const util::RegionVector<FieldDecl *> &parameters() const {
    return param_types_;
  }

  Expr *return_type() const { return ret_type_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::FunctionTypeRepr;
  }

 private:
  util::RegionVector<FieldDecl *> param_types_;
  Expr *ret_type_;
};

/// Map types
class MapTypeRepr : public Expr {
 public:
  MapTypeRepr(const SourcePosition &pos, Expr *key, Expr *val)
      : Expr(Kind::MapTypeRepr, pos), key_(key), val_(val) {}

  Expr *key() const { return key_; }

  Expr *val() const { return val_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::MapTypeRepr;
  }

 private:
  Expr *key_;
  Expr *val_;
};

/// Pointer type
class PointerTypeRepr : public Expr {
 public:
  PointerTypeRepr(const SourcePosition &pos, Expr *base)
      : Expr(Kind::PointerTypeRepr, pos), base_(base) {}

  Expr *base() const { return base_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::PointerTypeRepr;
  }

 private:
  Expr *base_;
};

/// Struct type
class StructTypeRepr : public Expr {
 public:
  StructTypeRepr(const SourcePosition &pos,
                 util::RegionVector<FieldDecl *> &&fields)
      : Expr(Kind::StructTypeRepr, pos), fields_(std::move(fields)) {}

  const util::RegionVector<FieldDecl *> &fields() const { return fields_; }

  static bool classof(const AstNode *node) {
    return node->kind() == Kind::StructTypeRepr;
  }

 private:
  util::RegionVector<FieldDecl *> fields_;
};

}  // namespace ast
}  // namespace tpl
