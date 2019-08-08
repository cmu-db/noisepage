#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/ast/ast_builder.h"
#include "execution/tpl_test.h"

#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sema/sema.h"
#include "execution/util/region_containers.h"

namespace terrier::execution::sema::test {

class SemaExprTest : public TplTest, public ast::test::TestAstBuilder {
 public:
  SemaExprTest() = default;

  void SetUp() override {
    TplTest::SetUp();
    ast::test::TestAstBuilder::SetUp();
  };

  void ResetErrorReporter() { error_reporter()->Reset(); }
};

struct TestCase {
  bool has_errors;
  std::string msg;
  ast::AstNode *tree;
};

// NOLINTNEXTLINE
TEST_F(SemaExprTest, LogicalOperationTest) {
  TestCase tests[] = {
      // Test: 1 and 2
      // Expectation: Error
      {true, "1 and 2 is not a valid logical operation", BinOp<parsing::Token::Type::AND>(IntLit(1), IntLit(2))},

      // Test: 1 and true
      // Expectation: Error
      {true, "1 and true is not a valid logical operation", BinOp<parsing::Token::Type::AND>(IntLit(1), BoolLit(true))},

      // Test: false and 2
      // Expectation: Error
      {true, "false and 1 is not a valid logical operation",
       BinOp<parsing::Token::Type::AND>(BoolLit(false), IntLit(2))},

      // Test: false and true
      // Expectation: Valid
      {false, "false and true is a valid logical operation",
       BinOp<parsing::Token::Type::AND>(BoolLit(false), BoolLit(true))},
  };

  for (const auto &test : tests) {
    Sema sema(ctx());
    bool has_errors = sema.Run(test.tree);
    EXPECT_EQ(test.has_errors, has_errors) << test.msg;
    ResetErrorReporter();
  }
}

// NOLINTNEXTLINE
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

// NOLINTNEXTLINE
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

// NOLINTNEXTLINE
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

}  // namespace terrier::execution::sema::test
