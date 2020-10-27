#include <string>

#include "execution/tpl_test.h"
#include "execution/vm/module.h"
#include "execution/vm/module_compiler.h"

namespace noisepage::execution::vm::test {

class BytecodeGeneratorTest : public TplTest {
 public:
  BytecodeGeneratorTest() : region_("test") {}

  util::Region *Region() { return &region_; }

 private:
  util::Region region_;
};

// NOLINTNEXTLINE
TEST_F(BytecodeGeneratorTest, SimpleTest) {
  {
    auto src = "fun test() -> bool { return true }";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    ASSERT_TRUE(module != nullptr);

    std::function<bool()> func;
    EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &func));
    EXPECT_TRUE(func());
  }

  {
    auto src = "fun test() -> bool { return false }";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    ASSERT_TRUE(module != nullptr);

    std::function<bool()> func;
    EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &func));
    EXPECT_FALSE(func());
  }

  {
    // Create a function that multiples an input unsigned 32-bit integer by 20
    auto src = R"(
    fun mul20(x: uint32) -> uint32 {
      var y: uint32 = 20
      return x * y
    })";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    ASSERT_TRUE(module != nullptr);

    std::function<uint32_t(uint32_t)> mul_20;
    EXPECT_TRUE(module->GetFunction("mul20", ExecutionMode::Interpret, &mul_20))
        << "Function 'mul20' not found in module";

    EXPECT_EQ(20u, mul_20(1));
    EXPECT_EQ(40u, mul_20(2));
    EXPECT_EQ(60u, mul_20(3));
  }
}

// NOLINTNEXTLINE
TEST_F(BytecodeGeneratorTest, BooleanEvaluationTest) {
  //
  // Boolean complement check.
  // Generate function: f(true) = -10, f(false) = 10
  //

  {
    auto src = R"(
    fun test(c: bool) -> int32 {
      if (!c) {
        return 10
      } else {
        return -10
      }
    })";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    ASSERT_TRUE(module != nullptr);

    std::function<int32_t(bool)> f;
    EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &f)) << "Function 'test' not found in module";
    EXPECT_EQ(10, f(false));
    EXPECT_EQ(-10, f(true));
  }

  //
  // Generate a function with a more complex boolean expression
  //

  {
    auto src = R"(
    fun test() -> bool {
      var x : int32 = 4
      var t : int32 = 8
      var f : int32 = 10
      return (f > 1 and x < 2) and (t < 100 or x < 3)
    })";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    ASSERT_TRUE(module != nullptr);

    std::function<bool()> f;
    EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &f)) << "Function 'test' not found in module";
    EXPECT_FALSE(f());
  }
}

// NOLINTNEXTLINE
TEST_F(BytecodeGeneratorTest, SimpleArithmeticTest) {
  const auto gen_compare_func = [](auto arg_type_name, auto dummy_arg, auto op, auto cb) {
    using Type = decltype(dummy_arg);
    auto src = fmt::format(R"(
      fun test(a: {0}, b: {0}) -> {0} {{
        return a {1} b
      }})",
                           arg_type_name, op);

    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    ASSERT_TRUE(module != nullptr);

    std::function<Type(Type, Type)> fn;
    ASSERT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &fn)) << "Function 'test' not found in module";

    // Test the function
    cb(fn);
  };

#define CMP_TEST(cpptype, tpltype, op)                                 \
  gen_compare_func(tpltype, cpptype{0}, #op, [](auto fn) {             \
    EXPECT_EQ(cpptype{1} op cpptype{1}, fn(cpptype{1}, cpptype{1}));   \
    EXPECT_EQ(cpptype{-1} op cpptype{1}, fn(cpptype{-1}, cpptype{1})); \
    EXPECT_EQ(cpptype{2} op cpptype{1}, fn(cpptype{2}, cpptype{1}));   \
  });

#define TEST_ALL_CMP(cpptype, tpltype) \
  CMP_TEST(cpptype, tpltype, +)        \
  CMP_TEST(cpptype, tpltype, -)        \
  CMP_TEST(cpptype, tpltype, *)        \
  CMP_TEST(cpptype, tpltype, /)        \
  CMP_TEST(cpptype, tpltype, %)

  TEST_ALL_CMP(int8_t, "int8")
  TEST_ALL_CMP(int16_t, "int16")
  TEST_ALL_CMP(int32_t, "int32")
  TEST_ALL_CMP(int64_t, "int64")

#undef TEST_ALL_CMP
#undef CMP_TEST
}

// NOLINTNEXTLINE
TEST_F(BytecodeGeneratorTest, ComparisonTest) {
  const auto gen_compare_func = [](auto arg_type_name, auto dummy_arg, auto op, auto cb) {
    using Type = decltype(dummy_arg);
    auto src = fmt::format(R"(
      fun test(a: {0}, b: {0}) -> bool {{
        return a {1} b
      }})",
                           arg_type_name, op);
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    ASSERT_TRUE(module != nullptr);

    std::function<bool(Type, Type)> fn;
    ASSERT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &fn)) << "Function 'test' not found in module";

    // Test the function
    cb(fn);
  };

#define CMP_TEST(cpptype, tpltype, op)                                 \
  gen_compare_func(tpltype, cpptype{0}, #op, [](auto fn) {             \
    EXPECT_EQ(cpptype{1} op cpptype{1}, fn(cpptype{1}, cpptype{1}));   \
    EXPECT_EQ(cpptype{-1} op cpptype{1}, fn(cpptype{-1}, cpptype{1})); \
    EXPECT_EQ(cpptype{2} op cpptype{1}, fn(cpptype{2}, cpptype{1}));   \
  });

#define TEST_ALL_CMP(cpptype, tpltype) \
  CMP_TEST(cpptype, tpltype, <)        \
  CMP_TEST(cpptype, tpltype, <=)       \
  CMP_TEST(cpptype, tpltype, ==)       \
  CMP_TEST(cpptype, tpltype, >)        \
  CMP_TEST(cpptype, tpltype, >=)       \
  CMP_TEST(cpptype, tpltype, !=)

  TEST_ALL_CMP(int8_t, "int8")
  TEST_ALL_CMP(int16_t, "int16")
  TEST_ALL_CMP(int32_t, "int32")
  TEST_ALL_CMP(int64_t, "int64")

#undef TEST_ALL_CMP
#undef CMP_TEST
}

// NOLINTNEXTLINE
TEST_F(BytecodeGeneratorTest, ParameterPassingTest) {
  auto src = R"(
    struct S {
      a: int
      b: int
    }
    fun test(s: *S) -> bool {
      s.a = 10
      s.b = s.a * 2
      return true
    })";
  auto compiler = ModuleCompiler();
  auto module = compiler.CompileToModule(src);
  ASSERT_TRUE(module != nullptr);

  struct S {
    int a_;
    int b_;
  };

  std::function<bool(S *)> f;
  EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &f)) << "Function 'test' not found in module";

  S s{.a_ = 0, .b_ = 0};
  EXPECT_TRUE(f(&s));
  EXPECT_EQ(10, s.a_);
  EXPECT_EQ(20, s.b_);
}

// NOLINTNEXTLINE
TEST_F(BytecodeGeneratorTest, FunctionTypeCheckTest) {
  {
    auto src = R"(
    fun test() -> nil {
      var a = 10
      return a
    })";

    auto compiler = ModuleCompiler();
    compiler.CompileToAst(src);
    EXPECT_TRUE(compiler.HasErrors());
  }

  {
    auto src = R"(
    fun test() -> int32 {
      return
    })";

    auto compiler = ModuleCompiler();
    compiler.CompileToAst(src);
    EXPECT_TRUE(compiler.HasErrors());
  }

  {
    auto src = R"(
    fun test() -> int32 {
      return 10
    })";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    ASSERT_TRUE(module != nullptr);

    std::function<int32_t()> f;
    EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &f)) << "Function 'test' not found in module";

    EXPECT_EQ(10, f());
  }

  {
    auto src = R"(
    fun test() -> int16 {
      var a: int16 = 20
      var b: int16 = 40
      return a * b
    })";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);

    std::function<int32_t()> f;
    EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &f)) << "Function 'test' not found in module";

    EXPECT_EQ(800, f());
  }
}

// NOLINTNEXTLINE
TEST_F(BytecodeGeneratorTest, FunctionTest) {
  auto src = R"(
    struct S {
      a: int
      b: int
    }
    fun f(s: *S) -> bool {
      s.b = s.a * 2
      return true
    }
    fun test(s: *S) -> bool {
      s.a = 10
      f(s)
      return true
    })";
  auto compiler = ModuleCompiler();
  auto module = compiler.CompileToModule(src);
  ASSERT_TRUE(module != nullptr);

  struct S {
    int a_;
    int b_;
  };

  std::function<bool(S *)> f;
  EXPECT_TRUE(module->GetFunction("test", ExecutionMode::Interpret, &f)) << "Function 'test' not found in module";

  S s{.a_ = 0, .b_ = 0};
  EXPECT_TRUE(f(&s));
  EXPECT_EQ(10, s.a_);
  EXPECT_EQ(20, s.b_);
}

}  // namespace noisepage::execution::vm::test
