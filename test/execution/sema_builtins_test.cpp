#include <vector>

#include "execution/ast/ast_builder.h"
#include "execution/sema/sema.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sema::test {

class SemaBuiltinTest : public TplTest, public ast::test::TestAstBuilder {
 public:
  bool Check(ast::AstNode *node) {
    sema::Sema sema(Ctx());
    return sema.Run(node);
  }

  void ResetErrorReporter() { ErrorReporter()->Reset(); }
};

// NOLINTNEXTLINE
TEST_F(SemaBuiltinTest, CheckSqlConversions) {
  //
  // Primitive integer to SQL integer
  //

  // int input to (int -> Integer) is valid
  {
    auto input1 = DeclVar(Ident("input"), PrimIntTypeRepr(), nullptr);
    auto result = Call<ast::Builtin::IntToSql>(DeclRef(input1));
    auto block = Block({DeclStmt(input1), ExprStmt(result)});
    EXPECT_EQ(false, Check(block));
    EXPECT_TRUE(result->GetType()->IsSpecificBuiltin(ast::BuiltinType::Integer));
    ResetErrorReporter();
  }

  // multiple int input to (int -> Integer) is invalid
  {
    auto input1 = DeclVar(Ident("input"), PrimIntTypeRepr(), nullptr);
    auto result = Call<ast::Builtin::IntToSql>(DeclRef(input1), DeclRef(input1));
    auto block = Block({DeclStmt(input1), ExprStmt(result)});
    EXPECT_EQ(true, Check(block));
    ResetErrorReporter();
  }

  // bool input to (int -> Integer) is invalid
  {
    auto input1 = DeclVar(Ident("input"), PrimBoolTypeRepr(), nullptr);
    auto result = Call<ast::Builtin::IntToSql>(DeclRef(input1));
    auto block = Block({DeclStmt(input1), ExprStmt(result)});
    EXPECT_EQ(true, Check(block));
    ResetErrorReporter();
  }

  //
  // Primitive boolean to SQL Boolean
  //

  // bool input to (bool -> Boolean) is valid
  {
    auto input1 = DeclVar(Ident("input"), PrimBoolTypeRepr(), nullptr);
    auto result = Call<ast::Builtin::BoolToSql>(DeclRef(input1));
    auto block = Block({DeclStmt(input1), ExprStmt(result)});
    EXPECT_EQ(false, Check(block));
    EXPECT_TRUE(result->GetType()->IsSpecificBuiltin(ast::BuiltinType::Boolean));
    ResetErrorReporter();
  }

  // integer input to (bool -> Boolean) is invalid
  {
    auto input1 = DeclVar(Ident("input"), PrimIntTypeRepr(), nullptr);
    auto result = Call<ast::Builtin::BoolToSql>(DeclRef(input1));
    auto block = Block({DeclStmt(input1), ExprStmt(result)});
    EXPECT_EQ(true, Check(block));
    ResetErrorReporter();
  }

  //
  // Primitive float to SQL Real
  //

  // float input to (float -> Real) is valid
  {
    auto input1 = DeclVar(Ident("input"), PrimFloatTypeRepr(), nullptr);
    auto result = Call<ast::Builtin::FloatToSql>(DeclRef(input1));
    auto block = Block({DeclStmt(input1), ExprStmt(result)});
    EXPECT_EQ(false, Check(block));
    EXPECT_TRUE(result->GetType()->IsSpecificBuiltin(ast::BuiltinType::Real));
    ResetErrorReporter();
  }

  // integer input to (float -> Real) is invalid
  {
    auto input1 = DeclVar(Ident("input"), PrimIntTypeRepr(), nullptr);
    auto result = Call<ast::Builtin::FloatToSql>(DeclRef(input1));
    auto block = Block({DeclStmt(input1), ExprStmt(result)});
    EXPECT_EQ(true, Check(block));
    ResetErrorReporter();
  }
}

// NOLINTNEXTLINE
TEST_F(SemaBuiltinTest, CheckTrigBuiltins) {
#define CHECK_TRIG(BUILTIN)                                                                         \
  {                                                                                                 \
    auto input1 = DeclVar(Ident("input"), RealSqlTypeRepr(), nullptr);                              \
    auto input2 = DeclVar(Ident("input2"), RealSqlTypeRepr(), nullptr);                             \
    auto input3 = DeclVar(Ident("input3"), IntegerSqlTypeRepr(), nullptr);                          \
    /* Check valid inputs */                                                                        \
    {                                                                                               \
      auto result = Call<BUILTIN>(DeclRef(input1));                                                 \
      auto block = Block({DeclStmt(input1), DeclStmt(input2), DeclStmt(input3), ExprStmt(result)}); \
      EXPECT_EQ(false, Check(block));                                                               \
      ResetErrorReporter();                                                                         \
    }                                                                                               \
    /* Check single invalid input */                                                                \
    {                                                                                               \
      auto result = Call<BUILTIN>(DeclRef(input3));                                                 \
      auto block = Block({DeclStmt(input1), DeclStmt(input2), DeclStmt(input3), ExprStmt(result)}); \
      EXPECT_EQ(true, Check(block));                                                                \
      ResetErrorReporter();                                                                         \
    }                                                                                               \
    /* Check wrong number of args input */                                                          \
    {                                                                                               \
      auto result = Call<BUILTIN>(DeclRef(input1), DeclRef(input2));                                \
      auto block = Block({DeclStmt(input1), DeclStmt(input2), DeclStmt(input3), ExprStmt(result)}); \
      EXPECT_EQ(true, Check(block));                                                                \
      ResetErrorReporter();                                                                         \
    }                                                                                               \
  }

  CHECK_TRIG(ast::Builtin::ACos);
  CHECK_TRIG(ast::Builtin::ASin);
  CHECK_TRIG(ast::Builtin::ATan);
  CHECK_TRIG(ast::Builtin::Cos);
  CHECK_TRIG(ast::Builtin::Cot);
  CHECK_TRIG(ast::Builtin::Sin);
  CHECK_TRIG(ast::Builtin::Tan);

#undef CHECK_TRIG

  // Atan2
  {
    auto left = DeclVar(Ident("left"), RealSqlTypeRepr(), nullptr);
    auto right = DeclVar(Ident("right"), RealSqlTypeRepr(), nullptr);
    auto result = Call<ast::Builtin::ATan2>(DeclRef(left), DeclRef(right));

    auto block = Block({DeclStmt(left), DeclStmt(right), ExprStmt(result)});
    EXPECT_EQ(false, Check(block));
  }
}

}  // namespace noisepage::execution::sema::test
