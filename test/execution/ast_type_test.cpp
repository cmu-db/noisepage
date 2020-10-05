#include <string>
#include <utility>

#include "execution/ast/context.h"
#include "execution/ast/type.h"
#include "execution/sema/error_reporter.h"
#include "execution/tpl_test.h"
#include "execution/util/region.h"

namespace terrier::execution::ast::test {

class TypeTest : public TplTest {
 public:
  TypeTest() : region_("type_test"), errors_(&region_), ctx_(&region_, &errors_) {}

  ast::Context *Ctx() { return &ctx_; }

  util::Region *Region() { return Ctx()->GetRegion(); }

  ast::Identifier Name(const std::string &s) { return Ctx()->GetIdentifier(s); }

  void CheckIsArrayType(Type *type, size_t num_elem, Type *elem_type) {
    EXPECT_TRUE(type->IsArrayType());

    // Check that the BuiltinType matches
    auto *array_type = type->As<ArrayType>();
    EXPECT_EQ(array_type->GetLength(), num_elem);
    EXPECT_EQ(array_type->GetElementType(), elem_type);
  }

  void CheckIsPadding(StructType *type, size_t offset, size_t expected_length) {
    auto identifier = Ctx()->GetIdentifier("__pad$" + std::to_string(offset) + "$");
    auto *field_type = type->LookupFieldByName(identifier);

    // Verify offset is correct
    EXPECT_TRUE(field_type != nullptr);
    EXPECT_EQ(type->GetOffsetOfFieldByName(identifier), offset);
    CheckIsArrayType(field_type, expected_length, ast::BuiltinType::Get(Ctx(), BuiltinType::Kind::Int8));
  }

 private:
  util::Region region_;
  sema::ErrorReporter errors_;
  ast::Context ctx_;
};

// NOLINTNEXTLINE
TEST_F(TypeTest, StructPaddingTest) {
  //
  // Summary: We create a TPL struct functionally equivalent to the C++ struct
  // 'Test' below. We expect the sizes to be the exact same, and the offsets of
  // each field to be the same.  In essence, we want TPL's layout engine to
  // replicate C/C++.
  //

  // clang-format off
  struct Test {
    bool a_;
    int64_t  b_;
    int8_t   c_;
    int32_t  d_;
    int8_t   e_;
    int16_t  f_;
    int64_t *g_;
    int8_t   h_;
  };
  // clang-format on

  auto fields = util::RegionVector<ast::Field>(
      {
          {Name("a"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool)},
          {Name("b"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int64)},
          {Name("c"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int8)},
          {Name("d"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int32)},
          {Name("e"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int8)},
          {Name("f"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int16)},
          {Name("g"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int64)->PointerTo()},
          {Name("h"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int8)},
      },
      Region());

  auto *type = ast::StructType::Get(std::move(fields));

  // Expect: [0-1] b, [2-7] pad, [8-15] int64_t, [16-17] int8_t_1, [18-19] pad,
  //         [20-23] int32_t, [24-25] int8_t_2, [26-27] int16_t, [28-31] pad, [32-39] p
  //         [40-41] int8_t, [42-47] pad
  EXPECT_EQ(sizeof(Test), type->GetSize());
  EXPECT_EQ(alignof(Test), type->GetAlignment());
  EXPECT_EQ(offsetof(Test, a_), type->GetOffsetOfFieldByName(Name("a")));
  EXPECT_EQ(offsetof(Test, b_), type->GetOffsetOfFieldByName(Name("b")));
  EXPECT_EQ(offsetof(Test, c_), type->GetOffsetOfFieldByName(Name("c")));
  EXPECT_EQ(offsetof(Test, d_), type->GetOffsetOfFieldByName(Name("d")));
  EXPECT_EQ(offsetof(Test, e_), type->GetOffsetOfFieldByName(Name("e")));
  EXPECT_EQ(offsetof(Test, f_), type->GetOffsetOfFieldByName(Name("f")));
  EXPECT_EQ(offsetof(Test, g_), type->GetOffsetOfFieldByName(Name("g")));
  EXPECT_EQ(offsetof(Test, h_), type->GetOffsetOfFieldByName(Name("h")));

  CheckIsPadding(type, 1, 7);
  CheckIsPadding(type, 17, 3);
  CheckIsPadding(type, 25, 1);
  CheckIsPadding(type, 28, 4);
  CheckIsPadding(type, 41, 7);
}

// NOLINTNEXTLINE
TEST_F(TypeTest, PrimitiveTypeCacheTest) {
  //
  // In any one Context, we must have a cache of types. First, check all the
  // integer types
  //

#define GEN_INT_TEST(Kind)                                                            \
  {                                                                                   \
    auto *type1 = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Kind);               \
    auto *type2 = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Kind);               \
    EXPECT_EQ(type1, type2) << "Received two different " #Kind " types from context"; \
  }
  GEN_INT_TEST(Int8);
  GEN_INT_TEST(Int16);
  GEN_INT_TEST(Int32);
  GEN_INT_TEST(Int64);
  GEN_INT_TEST(Int128);
  GEN_INT_TEST(Uint8);
  GEN_INT_TEST(Uint16);
  GEN_INT_TEST(Uint32);
  GEN_INT_TEST(Uint64);
  GEN_INT_TEST(Uint128);
#undef GEN_INT_TEST

  //
  // Try the floating point types ...
  //

#define GEN_FLOAT_TEST(Kind)                                                          \
  {                                                                                   \
    auto *type1 = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Kind);               \
    auto *type2 = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Kind);               \
    EXPECT_EQ(type1, type2) << "Received two different " #Kind " types from context"; \
  }
  GEN_FLOAT_TEST(Float32)
  GEN_FLOAT_TEST(Float64)
#undef GEN_FLOAT_TEST

  //
  // Really simple types
  //

  EXPECT_EQ(ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool));
  EXPECT_EQ(ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Nil), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Nil));
}

// NOLINTNEXTLINE
TEST_F(TypeTest, StructTypeCacheTest) {
  //
  // Create two structurally equivalent types and ensure only one struct
  // instantiation is created in the context
  //

  {
    auto *type1 = ast::StructType::Get(
        util::RegionVector<ast::Field>({{Name("a"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool)},
                                        {Name("b"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int64)}},
                                       Region()));

    auto *type2 = ast::StructType::Get(
        util::RegionVector<ast::Field>({{Name("a"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool)},
                                        {Name("b"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int64)}},
                                       Region()));

    EXPECT_EQ(type1, type2) << "Received two different pointers to same struct type";
  }

  //
  // Create two **DIFFERENT** structures and ensure they have different
  // instantiations in the context
  //

  {
    auto *type1 = ast::StructType::Get(
        util::RegionVector<ast::Field>({{Name("a"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool)}}, Region()));

    auto *type2 = ast::StructType::Get(
        util::RegionVector<ast::Field>({{Name("a"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int64)}}, Region()));

    EXPECT_NE(type1, type2) << "Received two equivalent pointers for different struct types";
  }
}

// NOLINTNEXTLINE
TEST_F(TypeTest, PointerTypeCacheTest) {
  //
  // Pointer types should also be cached. Thus, two *int8_t types should have
  // pointer equality in a given context
  //

#define GEN_INT_TEST(Kind)                                                             \
  {                                                                                    \
    auto *type1 = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Kind)->PointerTo();   \
    auto *type2 = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Kind)->PointerTo();   \
    EXPECT_EQ(type1, type2) << "Received two different *" #Kind " types from context"; \
  }
  GEN_INT_TEST(Int8);
  GEN_INT_TEST(Int16);
  GEN_INT_TEST(Int32);
  GEN_INT_TEST(Int64);
  GEN_INT_TEST(Int128);
  GEN_INT_TEST(Uint8);
  GEN_INT_TEST(Uint16);
  GEN_INT_TEST(Uint32);
  GEN_INT_TEST(Uint64);
  GEN_INT_TEST(Uint128);
#undef GEN_INT_TEST

  //
  // Try to create a pointer to the same struct and ensure the they point to the
  // same type instance
  //

  {
    auto *struct_type = ast::StructType::Get(
        util::RegionVector<ast::Field>({{Name("a"), ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool)}}, Region()));
    EXPECT_EQ(struct_type->PointerTo(), struct_type->PointerTo());
  }
}

// NOLINTNEXTLINE
TEST_F(TypeTest, FunctionTypeCacheTest) {
  //
  // Check that even function types are cached in the context. In the first
  // test, both functions have type: (bool)->bool
  //

  {
    auto *bool_type_1 = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool);
    auto *type1 =
        ast::FunctionType::Get(util::RegionVector<ast::Field>({{Name("a"), bool_type_1}}, Region()), bool_type_1);

    auto *bool_type_2 = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool);
    auto *type2 =
        ast::FunctionType::Get(util::RegionVector<ast::Field>({{Name("a"), bool_type_2}}, Region()), bool_type_2);

    EXPECT_EQ(type1, type2);
  }

  //
  // In this test, the two functions have different types, and hence, should not
  // cache to the same function type instance. The first function has type:
  // (bool)->bool, but the second has type (int32)->int32
  //

  {
    // The first function has type: (bool)->bool
    auto *bool_type = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Bool);
    auto *type1 = ast::FunctionType::Get(util::RegionVector<ast::Field>({{Name("a"), bool_type}}, Region()), bool_type);

    auto *int_type = ast::BuiltinType::Get(Ctx(), ast::BuiltinType::Int32);
    auto *type2 = ast::FunctionType::Get(util::RegionVector<ast::Field>({{Name("a"), int_type}}, Region()), int_type);

    EXPECT_NE(type1, type2);
  }
}

}  // namespace terrier::execution::ast::test
