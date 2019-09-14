#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/builtins.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sema/error_reporter.h"
#include "execution/util/region_containers.h"

namespace terrier::execution::ast::test {

class TestAstBuilder {
 public:
  /**
   * Constructor
   */
  TestAstBuilder() = default;

  /**
   * Setup class members
   */
  virtual void SetUp() {
    region_ = std::make_unique<util::Region>("test");
    error_reporter_ = std::make_unique<sema::ErrorReporter>(region_.get());
    ctx_ = std::make_unique<Context>(region_.get(), error_reporter_.get());
  }

  /**
   * @return the ast context
   */
  Context *Ctx() { return ctx_.get(); }

  /**
   * @return the region
   */
  util::Region *Region() { return region_.get(); }

  /**
   * @return the error reporter
   */
  sema::ErrorReporter *GetErrorReporter() { return error_reporter_.get(); }

  /**
   * Make an Identifier
   */
  Identifier Ident(const std::string &s) { return Ctx()->GetIdentifier(s); }

  /**
   * Make an Identifier expression
   */
  Expr *IdentExpr(Identifier ident) { return NodeFactory()->NewIdentifierExpr(empty_, ident); }

  /**
   * Make an Identifier expression
   */
  Expr *IdentExpr(const std::string &s) { return IdentExpr(Ident(s)); }

  /**
   * Make a bool literal
   */
  Expr *BoolLit(bool b) { return NodeFactory()->NewBoolLiteral(empty_, b); }

  /**
   * Make an int litera;
   */
  Expr *IntLit(int32_t i) { return NodeFactory()->NewIntLiteral(empty_, i); }

  /**
   * Make a float literal
   */
  Expr *FloatLit(float i) { return NodeFactory()->NewFloatLiteral(empty_, i); }

  /**
   * Make a binary op expression
   */
  template <parsing::Token::Type OP>
  Expr *BinOp(Expr *left, Expr *right) {
    return NodeFactory()->NewBinaryOpExpr(empty_, OP, left, right);
  }

  /**
   * Make a comparison expression
   */
  template <parsing::Token::Type OP>
  Expr *Cmp(Expr *left, Expr *right) {
    TERRIER_ASSERT(parsing::Token::IsCompareOp(OP), "Not a comparison");
    return NodeFactory()->NewComparisonOpExpr(empty_, OP, left, right);
  }

  /**
   * Make a == check expression
   */
  Expr *CmpEq(Expr *left, Expr *right) { return Cmp<parsing::Token::Type::EQUAL_EQUAL>(left, right); }

  /**
   * Make a !=  check expression
   */
  Expr *CmpNe(Expr *left, Expr *right) { return Cmp<parsing::Token::Type::BANG_EQUAL>(left, right); }

  /**
   * Make a < check expression
   */
  Expr *CmpLt(Expr *left, Expr *right) { return Cmp<parsing::Token::Type::LESS>(left, right); }

  /**
   * Make a member expression
   */
  Expr *Field(Expr *obj, Expr *field) { return NodeFactory()->NewMemberExpr(empty_, obj, field); }

  /**
   * Make an variable declaration with inferred type
   */
  VariableDecl *DeclVar(Identifier name, Expr *init) { return DeclVar(name, nullptr, init); }

  /**
   * Make an variable declaration with explicit type
   */
  VariableDecl *DeclVar(Identifier name, Expr *type_repr, Expr *init) {
    return NodeFactory()->NewVariableDecl(empty_, name, type_repr, init);
  }

  /**
   * Get the identifier of a declared object
   */
  Expr *DeclRef(Decl *decl) { return IdentExpr(decl->Name()); }

  /**
   * Convert declaration to statement
   */
  Stmt *DeclStmt(Decl *decl) { return NodeFactory()->NewDeclStmt(decl); }

  /**
   * Construct a block statement
   */
  Stmt *Block(std::initializer_list<Stmt *> stmts) {
    util::RegionVector<Stmt *> region_stmts(stmts.begin(), stmts.end(), Region());
    return NodeFactory()->NewBlockStmt(empty_, empty_, std::move(region_stmts));
  }

  /**
   * Convert expression to statement
   */
  Stmt *ExprStmt(Expr *expr) { return NodeFactory()->NewExpressionStmt(expr); }

  /**
   * Get pointer to the base type
   */
  Expr *PtrType(Expr *base) { return NodeFactory()->NewPointerType(empty_, base); }

  /**
   * Get builtin type expression
   */
  template <BuiltinType::Kind BUILTIN>
  Expr *BuiltinTypeRepr() {
    return IdentExpr(BuiltinType::Get(Ctx(), BUILTIN)->TplName());
  }

  /**
   * Get an int32 type
   */
  Expr *PrimIntTypeRepr() { return BuiltinTypeRepr<BuiltinType::Int32>(); }

  /**
   * Get an float type
   */
  Expr *PrimFloatTypeRepr() { return BuiltinTypeRepr<BuiltinType::Float32>(); }

  /**
   * Get a bool type
   */
  Expr *PrimBoolTypeRepr() { return BuiltinTypeRepr<BuiltinType::Bool>(); }

  /**
   * Get an Integer type
   */
  Expr *IntegerSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::Integer>(); }

  /**
   * Get a Real type
   */
  Expr *RealSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::Real>(); }

  /**
   * Get a StringVal type
   */
  Expr *StringSqlTypeRepr() { return BuiltinTypeRepr<BuiltinType::StringVal>(); }

  /**
   * Get an array type
   */
  Expr *ArrayTypeRepr(Expr *type) { return NodeFactory()->NewArrayType(empty_, nullptr, type); }

  /**
   * Make an array indexing expression
   */
  Expr *ArrayIndex(Expr *arr, Expr *idx) { return NodeFactory()->NewIndexExpr(empty_, arr, idx); }

  /**
   * Call a builtin function with the given arguments
   */
  template <Builtin BUILTIN, typename... Args>
  CallExpr *Call(Args... args) {
    auto fn = IdentExpr(Builtins::GetFunctionName(BUILTIN));
    auto call_args = util::RegionVector<Expr *>({std::forward<Args>(args)...}, Region());
    return NodeFactory()->NewBuiltinCallExpr(fn, std::move(call_args));
  }

 private:
  AstNodeFactory *NodeFactory() { return Ctx()->NodeFactory(); }

 private:
  std::unique_ptr<util::Region> region_{nullptr};
  std::unique_ptr<sema::ErrorReporter> error_reporter_{nullptr};
  std::unique_ptr<Context> ctx_{nullptr};
  SourcePosition empty_{0, 0};
};

}  // namespace terrier::execution::ast::test
