#include <algorithm>
#include <limits>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "ips4o/ips4o.hpp"

#include "execution/tpl_test.h"

#include "execution/sql/sorter.h"
#include "execution/sql/value.h"
#include "execution/vm/module.h"
#include "execution/vm/module_compiler.h"

namespace terrier::execution::vm::test {

//
// These tests use the trampoline to call into bytecode functions.
// TODO(pmenon): We need way more fucking tests for this ...
//

class BytecodeTrampolineTest : public TplTest {
 protected:
  void *GetTrampoline(const vm::Module &module, const std::string &func_name) {
    return module.GetBytecodeImpl(module.GetFuncInfoByName(func_name)->id());
  }
};

// NOLINTNEXTLINE
TEST_F(BytecodeTrampolineTest, VoidFunctionTest) {
  auto src = "fun test() -> nil { }";
  auto compiler = ModuleCompiler();
  auto module = compiler.CompileToModule(src);

  EXPECT_FALSE(compiler.HasErrors());

  auto fn = reinterpret_cast<void (*)()>(GetTrampoline(*module, "test"));

  fn();
}

// NOLINTNEXTLINE
TEST_F(BytecodeTrampolineTest, BooleanFunctionTest) {
  auto src = "fun lt(a: int32, b: int32) -> bool { return a < b }";
  auto compiler = ModuleCompiler();
  auto module = compiler.CompileToModule(src);

  EXPECT_FALSE(compiler.HasErrors());

  auto less_than = reinterpret_cast<bool (*)(i32, i32)>(GetTrampoline(*module, "lt"));

  EXPECT_EQ(true, less_than(1, 2));
  EXPECT_EQ(false, less_than(2, 1));
}

// NOLINTNEXTLINE
TEST_F(BytecodeTrampolineTest, IntFunctionTest) {
  {
    auto src = "fun test() -> int32 { return 10 }";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<i32 (*)()>(GetTrampoline(*module, "test"));

    EXPECT_EQ(10, fn());
  }

  // Add function
  {
    auto src = "fun add2(a: int32, b: int32) -> int32 { return a + b }";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<i32 (*)(i32, i32)>(GetTrampoline(*module, "add2"));

    EXPECT_EQ(20, fn(10, 10));
    EXPECT_EQ(10, fn(0, 10));
    EXPECT_EQ(10, fn(10, 0));
    EXPECT_EQ(0, fn(0, 0));
  }

  // Sub function
  {
    auto src = "fun sub3(a: int32, b: int32, c: int32) -> int32 { return a - b - c }";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<i32 (*)(i32, i32, i32)>(GetTrampoline(*module, "sub3"));

    EXPECT_EQ(-10, fn(10, 10, 10));
    EXPECT_EQ(10, fn(30, 10, 10));
    EXPECT_EQ(0, fn(0, 0, 0));
  }
}

// NOLINTNEXTLINE
TEST_F(BytecodeTrampolineTest, BigIntFunctionTest) {
  {
    auto src = R"(
    fun mul3(a: int64, b: int64, c: int64) -> int64 {
      return a * b * c
    })";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<i64 (*)(i64, i64, i64)>(GetTrampoline(*module, "mul3"));

    EXPECT_EQ(6, fn(1, 2, 3));
    EXPECT_EQ(-6, fn(-1, 2, 3));
    EXPECT_EQ(0, fn(0, 2, 3));
  }
}

// NOLINTNEXTLINE
TEST_F(BytecodeTrampolineTest, VoidReturnTest) {
  {
    auto src = R"(
    fun mul2(a: *int64, b: *int64, ret: *int64) -> nil {
      *ret = (*a) * (*b)
    })";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);

    EXPECT_FALSE(compiler.HasErrors());

    auto fn = reinterpret_cast<void (*)(i64 *, i64 *, i64 *)>(GetTrampoline(*module, "mul2"));

    i64 a = 2, b = 3;
    i64 ret = 0;

    fn(&a, &b, &ret);
    EXPECT_EQ(6, ret);

    a = 10, b = -10, ret = 0;
    fn(&a, &b, &ret);
    EXPECT_EQ(-100, ret);
  }
}

// NOLINTNEXTLINE
TEST_F(BytecodeTrampolineTest, CodeGenComparisonFunctionSorterTest) {
  //
  // Test 1: Sort a list of signed 32-bit signed integers using a generated TPL
  //         function. The list contains integers in the range [0, 100] and
  //         will be sorted in ascending order.
  //

  {
    const u32 nelems = 100;
    std::vector<i32> numbers(nelems);
    std::random_device random;
    std::generate(numbers.begin(), numbers.end(), [&random]() { return random() % 100; });

    // Generate the comparison function that sorts ascending
    auto src = "fun compare(a: int32, b: int32) -> int32 { return a - b }";

    // Compile
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    EXPECT_FALSE(compiler.HasErrors());
    auto compare = reinterpret_cast<i32 (*)(const i32, const i32)>(GetTrampoline(*module, "compare"));
    EXPECT_TRUE(compare != nullptr);

    // Try to sort using the generated comparison function
    ips4o::sort(numbers.begin(), numbers.end(),
                // NOLINTNEXTLINE
                [compare](const auto &a, const auto &b) { return compare(a, b) < 0; });

    // Verify
    EXPECT_TRUE(std::is_sorted(numbers.begin(), numbers.end()));
  }

  //
  // Test 2: Sort a list of custom structures using a custom generated TPL
  //         function. Each struct is composed of four 32-bit integers, a, b, c,
  //         and d. All integers are in the range [0, 100]. The list is sorted
  //         ascending by the 'c' field.
  //

  {
    struct S {
      i32 a, b, c, d;
      S(i32 a, i32 b, i32 c, i32 d) : a(a), b(b), c(c), d(d) {}
    };

    const u32 nelems = 100;
    std::vector<S> elems;
    std::random_device random;
    for (u32 i = 0; i < nelems; i++) {
      elems.emplace_back(random() % 5, random() % 10, random() % 100, random() % 1000);
    }

    // Generate the comparison function that sorts ascending by S.c
    auto src = R"(
    struct S {
      a: int32
      b: int32
      c: int32
      d: int32
    }
    fun compare(a: *S, b: *S) -> bool { return a.c < b.c })";

    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    EXPECT_FALSE(compiler.HasErrors());
    auto compare = reinterpret_cast<bool (*)(const S *, const S *)>(GetTrampoline(*module, "compare"));
    EXPECT_TRUE(compare != nullptr);

    // Try to sort using the generated comparison function
    ips4o::sort(elems.begin(), elems.end(), [compare](const auto &a, const auto &b) { return compare(&a, &b); });

    // Verify
    EXPECT_TRUE(std::is_sorted(elems.begin(), elems.end(), [](const auto &a, const auto &b) { return a.c < b.c; }));
  }
}

// NOLINTNEXTLINE
TEST_F(BytecodeTrampolineTest, DISABLED_PerfGenComparisonForSortTest) {
  // Try sorting through trampoline
  auto bench_trampoline = [this](auto &vec) {
    auto src = "fun compare(a: int32, b: int32) -> int32 { return a - b }";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    auto compare = reinterpret_cast<i32 (*)(const i32, const i32)>(GetTrampoline(*module, "compare"));

    util::Timer<std::milli> timer;
    timer.Start();
    ips4o::sort(vec.begin(), vec.end(),
                // NOLINTNEXTLINE
                [compare](const auto a, const auto b) { return compare(a, b) < 0; });
    timer.Stop();
    return timer.elapsed();
  };

  UNUSED auto bench_func = [](auto &vec) {
    auto src = "fun compare(a: int32, b: int32) -> int32 { return a - b }";
    auto compiler = ModuleCompiler();
    auto module = compiler.CompileToModule(src);
    std::function<i32(const i32, const i32)> compare;
    EXPECT_TRUE(module->GetFunction("compare", ExecutionMode::Interpret, &compare));

    util::Timer<std::milli> timer;
    timer.Start();
    ips4o::sort(vec.begin(), vec.end(),
                // NOLINTNEXTLINE
                [&compare](const auto a, const auto b) { return compare(a, b) < 0; });
    timer.Stop();
    return timer.elapsed();
  };

  UNUSED auto bench_std = [](auto &vec) {
    auto compiler = ModuleCompiler();
    util::Timer<std::milli> timer;
    timer.Start();
    ips4o::sort(vec.begin(), vec.end(),
                // NOLINTNEXTLINE
                [](const auto &a, const auto &b) { return a < b; });
    timer.Stop();
    return timer.elapsed();
  };

  const u32 nelems = 10000000;
  std::vector<i32> numbers(nelems);
  i32 x = 0;
  UNUSED std::random_device random;
  std::generate(numbers.begin(), numbers.end(), [&x]() { return x++; });

  auto num2 = numbers;
  auto num3 = numbers;

  auto tramp_ms = bench_trampoline(numbers);
  auto func_ms = bench_func(num2);
  auto std_ms = bench_std(num3);

  std::cout << "Trampoline: " << tramp_ms << " ms, ";
  std::cout << "Function: " << func_ms << " ms, ";
  std::cout << "Std: " << std_ms << " ms" << std::endl;
}

}  // namespace terrier::execution::vm::test
