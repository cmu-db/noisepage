#include <string>
#include <utility>
#include <vector>

#include "execution/tpl_test.h"  // NOLINT

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sema/sema.h"
#include "execution/sql/execution_structures.h"
#include "execution/util/region_containers.h"

namespace tpl::sema::test {

class SemaExprTest : public TplTest {
 public:
  SemaExprTest() : region_("test"), error_reporter_(&region_), ctx_(&region_, &error_reporter_) {
    sql::ExecutionStructures::Instance();
  }

  util::Region *region() { return &region_; }
  ErrorReporter *error_reporter() { return &error_reporter_; }
  ast::Context *ctx() { return &ctx_; }
  ast::AstNodeFactory *node_factory() { return ctx_.node_factory(); }

  ast::Identifier Ident(const std::string &s) { return ctx()->GetIdentifier(s); }

  ast::Expr *IdentExpr(const std::string &s) { return node_factory()->NewIdentifierExpr(empty_, Ident(s)); }

  ast::Expr *BoolLit(bool b) { return node_factory()->NewBoolLiteral(empty_, b); }

  ast::Expr *IntLit(u32 i) { return node_factory()->NewIntLiteral(empty_, i); }

  ast::Expr *FloatLit(f32 i) { return node_factory()->NewFloatLiteral(empty_, i); }

  template <parsing::Token::Type OP>
  ast::Expr *Cmp(ast::Expr *left, ast::Expr *right) {
    TPL_ASSERT(parsing::Token::IsCompareOp(OP), "Not a comparison");
    return node_factory()->NewComparisonOpExpr(empty_, OP, left, right);
  }

  ast::Expr *CmpEq(ast::Expr *left, ast::Expr *right) { return Cmp<parsing::Token::Type::EQUAL_EQUAL>(left, right); }
  ast::Expr *CmpNe(ast::Expr *left, ast::Expr *right) { return Cmp<parsing::Token::Type::BANG_EQUAL>(left, right); }
  ast::Expr *CmpLt(ast::Expr *left, ast::Expr *right) { return Cmp<parsing::Token::Type::LESS>(left, right); }

  ast::Expr *Field(ast::Expr *obj, ast::Expr *field) { return node_factory()->NewMemberExpr(empty_, obj, field); }

  ast::VariableDecl *DeclVar(ast::Identifier name, ast::Expr *init) { return DeclVar(name, nullptr, init); }

  ast::VariableDecl *DeclVar(ast::Identifier name, ast::Expr *type_repr, ast::Expr *init) {
    return node_factory()->NewVariableDecl(empty_, name, type_repr, init);
  }

  ast::Stmt *DeclStmt(ast::Decl *decl) { return node_factory()->NewDeclStmt(decl); }

  ast::Stmt *Block(std::initializer_list<ast::Stmt *> stmts) {
    util::RegionVector<ast::Stmt *> region_stmts(stmts.begin(), stmts.end(), region());
    return node_factory()->NewBlockStmt(empty_, empty_, std::move(region_stmts));
  }

  ast::Stmt *ExprStmt(ast::Expr *expr) { return node_factory()->NewExpressionStmt(expr); }

  ast::Expr *PtrType(ast::Expr *base) { return node_factory()->NewPointerType(empty_, base); }

  ast::Expr *ArrayTypeRepr(ast::Expr *type) { return node_factory()->NewArrayType(empty_, nullptr, type); }

  ast::Expr *ArrayIndex(ast::Expr *arr, ast::Expr *idx) { return node_factory()->NewIndexExpr(empty_, arr, idx); }

  void ResetErrorReporter() { error_reporter()->Reset(); }

 private:
  util::Region region_;
  ErrorReporter error_reporter_;
  ast::Context ctx_;

  SourcePosition empty_{0, 0};
};

struct TestCase {
  bool has_errors;
  std::string msg;
  ast::AstNode *tree;
};

TEST_F(SemaExprTest, LogicalOperationTest) {
  SourcePosition empty{0, 0};

  TestCase tests[] = {
      // Test: 1 and 2
      // Expectation: Error
      {true, "1 and 2 is not a valid logical operation",
       node_factory()->NewBinaryOpExpr(empty, parsing::Token::Type::AND, node_factory()->NewIntLiteral(empty, 1),
                                       node_factory()->NewIntLiteral(empty, 2))},

      // Test: 1 and true
      // Expectation: Error
      {true, "1 and true is not a valid logical operation",
       node_factory()->NewBinaryOpExpr(empty, parsing::Token::Type::AND, node_factory()->NewIntLiteral(empty, 1),
                                       node_factory()->NewBoolLiteral(empty, true))},

      // Test: false and 2
      // Expectation: Error
      {true, "false and 1 is not a valid logical operation",
       node_factory()->NewBinaryOpExpr(empty, parsing::Token::Type::AND, node_factory()->NewBoolLiteral(empty, false),
                                       node_factory()->NewIntLiteral(empty, 2))},

      // Test: false and true
      // Expectation: Valid
      {false, "false and true is a valid logical operation",
       node_factory()->NewBinaryOpExpr(empty, parsing::Token::Type::AND, node_factory()->NewBoolLiteral(empty, false),
                                       node_factory()->NewBoolLiteral(empty, true))},
  };

  for (const auto &test : tests) {
    Sema sema(ctx());
    bool has_errors = sema.Run(test.tree);
    EXPECT_EQ(test.has_errors, has_errors) << test.msg;
    ResetErrorReporter();
  }
}

TEST_F(SemaExprTest, ComparisonOperationWithImplicitCastTest) {
  // clang-format off
TestCase tests[] = {
    // Test: Compare a primitive int32 with a SQL integer
    // Expectation: Valid
    {false, "SQL integers should be comparable to native integers",
     Block({
               DeclStmt(DeclVar(Ident("sqlInt"), IdentExpr("Integer"))),    // var sqlInt: Integer
               DeclStmt(DeclVar(Ident("i"), IntLit(10))),                   // var i = 10
               ExprStmt(CmpLt(IdentExpr("sqlInt"), IdentExpr("i"))),        // sqlInt < i
           })},

    // Test: Compare a primitive int32 with a SQL integer
    // Expectation: Valid
    {false, "SQL integers should be comparable to native integers",
     Block({
               DeclStmt(DeclVar(Ident("sqlInt"), IdentExpr("Integer"))),    // var sqlInt: Integer
               DeclStmt(DeclVar(Ident("i"), IntLit(10))),                   // var i = 10
               ExprStmt(CmpLt(IdentExpr("i"), IdentExpr("sqlInt"))),        // i < sqlInt
           })},

    // Test: Compare a primitive bool with a SQL integer
    // Expectation: Invalid
    {true, "SQL integers should not be comparable to native boolean values",
     Block({
               DeclStmt(DeclVar(Ident("sqlInt"), IdentExpr("Integer"))),    // var sqlInt: Integer
               DeclStmt(DeclVar(Ident("b"), BoolLit(false))),               // var b = false
               ExprStmt(CmpLt(IdentExpr("b"), IdentExpr("sqlInt"))),        // b < sqlInt
           })},
};
  // clang-format on

  for (const auto &test : tests) {
    Sema sema(ctx());
    bool has_errors = sema.Run(test.tree);
    EXPECT_EQ(test.has_errors, has_errors) << test.msg;
    ResetErrorReporter();
  }
}

TEST_F(SemaExprTest, ComparisonOperationWithPointersTest) {
  // clang-format off
TestCase tests[] = {
    // Test: Compare a primitive int32 with an integer
    // Expectation: Invalid
    {true, "Integers should not be comparable to pointers",
     Block({
               DeclStmt(DeclVar(Ident("i"), IntLit(10))),                           // var i = 10
               DeclStmt(DeclVar(Ident("ptr"), PtrType(IdentExpr("int32")))),        // var ptr: *int32
               ExprStmt(CmpEq(IdentExpr("i"), IdentExpr("ptr"))),                   // i == ptr
           })},

    // Test: Compare pointers to primitive int32 and float32
    // Expectation: Valid
    {true, "Pointers of different types should not be comparable",
     Block({
               DeclStmt(DeclVar(Ident("ptr1"), PtrType(IdentExpr("int32")))),       // var ptr1: *int32
               DeclStmt(DeclVar(Ident("ptr2"), PtrType(IdentExpr("float32")))),     // var ptr2: *float32
               ExprStmt(CmpEq(IdentExpr("ptr1"), IdentExpr("ptr"))),                // ptr1 == ptr2
           })},

    // Test: Compare pointers to the same type
    // Expectation: Valid
    {false, "Pointers to the same type should be comparable",
     Block({
               DeclStmt(DeclVar(Ident("ptr1"), PtrType(IdentExpr("float32")))),     // var ptr1: *float32
               DeclStmt(DeclVar(Ident("ptr2"), PtrType(IdentExpr("float32")))),     // var ptr2: *float32
               ExprStmt(CmpEq(IdentExpr("ptr1"), IdentExpr("ptr2"))),               // ptr1 == ptr2
           })},

    // Test: Compare pointers to the same type, but using a relational op
    // Expectation: Invalid
    {true, "Pointers to the same type should be comparable",
     Block({
               DeclStmt(DeclVar(Ident("ptr1"), PtrType(IdentExpr("float32")))),     // var ptr1: *float32
               DeclStmt(DeclVar(Ident("ptr2"), PtrType(IdentExpr("float32")))),     // var ptr2: *float32
               ExprStmt(CmpLt(IdentExpr("ptr1"), IdentExpr("ptr2"))),               // ptr1 == ptr2
           })},
};
  // clang-format on

  for (const auto &test : tests) {
    Sema sema(ctx());
    bool has_errors = sema.Run(test.tree);
    EXPECT_EQ(test.has_errors, has_errors) << test.msg;
    ResetErrorReporter();
  }
}

TEST_F(SemaExprTest, ArrayIndexTest) {
  // clang-format off
TestCase tests[] = {
    // Test: Perform an array index using an integer literal
    // Expectation: Valid
    {false, "Array indexes can support literal indexes",
     Block({
               DeclStmt(DeclVar(Ident("arr"), ArrayTypeRepr(IdentExpr("int32")))),    // var arr: []int32
               ExprStmt(ArrayIndex(IdentExpr("arr"), IntLit(10))),                    // arr[10]
           })},

    // Test: Perform an array index using an integer variable
    // Expectation: Valid
    {false, "Array indexes can support variable integer indexes",
     Block({
               DeclStmt(DeclVar(Ident("arr"), ArrayTypeRepr(IdentExpr("int32")))),    // var arr: []int32
               DeclStmt(DeclVar(Ident("i"), IntLit(10))),                             // var i = 10
               ExprStmt(ArrayIndex(IdentExpr("arr"), IdentExpr("i"))),                // arr[i]
           })},

    // Test: Perform an array index using an floating-point variable
    // Expectation: Invalid
    {true, "Array indexes must be integer values",
     Block({
               DeclStmt(DeclVar(Ident("arr"), ArrayTypeRepr(IdentExpr("int32")))),    // var arr: []int32
               DeclStmt(DeclVar(Ident("i"), FloatLit(10.0))),                         // var i: float32 = 10.0
               ExprStmt(ArrayIndex(IdentExpr("arr"), IdentExpr("i"))),                // arr[i]
           })},

    // Test: Perform an array index using a SQL integer
    // Expectation: Invalid
    {true, "Array indexes must be integer values",
     Block({
               DeclStmt(DeclVar(Ident("arr"), ArrayTypeRepr(IdentExpr("int32")))),    // var arr: []int32
               DeclStmt(DeclVar(Ident("i"), IdentExpr("Integer"), nullptr)),          // var i: Integer
               ExprStmt(ArrayIndex(IdentExpr("arr"), IdentExpr("i"))),                // arr[i]
           })},
};
  // clang-format on

  for (const auto &test : tests) {
    Sema sema(ctx());
    bool has_errors = sema.Run(test.tree);
    EXPECT_EQ(test.has_errors, has_errors) << test.msg;
    ResetErrorReporter();
  }
}

}  // namespace tpl::sema::test
