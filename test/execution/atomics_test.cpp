#include <random>
#include <string>

#include "common/macros.h"
#include "common/worker_pool.h"
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"
#include "execution/compiler/codegen.h"
#include "execution/compiler/compiler.h"
#include "execution/compiler/function_builder.h"
#include "execution/sema/error_reporter.h"
#include "execution/tpl_test.h"
#include "execution/util/region.h"
#include "execution/util/region_containers.h"
#include "execution/vm/module.h"
#include "execution/vm/vm_defs.h"
#include "test_util/multithread_test_util.h"

namespace noisepage::execution::test {

class AtomicsTest : public TplTest {
 public:
  AtomicsTest() : region_("atomics_test"), pos_() {}

  util::Region *Region() { return &region_; }

  const SourcePosition &EmptyPos() const { return pos_; }

  util::Region region_;
  SourcePosition pos_;

  template <typename T>
  void AndOrTest(const std::string &src) {
    // Setup the compilation environment
    sema::ErrorReporter error_reporter(&region_);
    ast::AstNodeFactory factory(&region_);
    ast::Context context(&region_, &error_reporter);

    // Compile it...
    auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
    auto module = compiler::Compiler::RunCompilationSimple(input);
    ASSERT_FALSE(module == nullptr);

    // The function should exist
    std::function<T(T *, T)> atomic_and;
    EXPECT_TRUE(module->GetFunction("atomic_and", vm::ExecutionMode::Interpret, &atomic_and));

    // The function should exist
    std::function<T(T *, T)> atomic_or;
    EXPECT_TRUE(module->GetFunction("atomic_or", vm::ExecutionMode::Interpret, &atomic_or));

    /*=========================
     *= Run correctness tests =
     *=========================
     */

    std::default_random_engine generator;
    const uint32_t num_iters = 100;
    const uint32_t num_cycles = 1000;
    const uint32_t num_threads = sizeof(T) * 8;  // Number of bits in test
    common::WorkerPool thread_pool(num_threads, {});

    for (uint32_t iter = 0; iter < num_iters; ++iter) {
      std::atomic<T> target = 0;
      auto workload = [&](uint32_t thread_id) {
        auto mask = static_cast<T>(1) << thread_id;
        auto inv_mask = ~mask;
        ASSERT_NE(mask, 0);

        T before;
        for (uint32_t i = 0; i < num_cycles; ++i) {
          // Set it
          before = atomic_or(reinterpret_cast<T *>(&target), mask);
          EXPECT_EQ(before & mask, 0);
          EXPECT_EQ(target.load() & mask, mask);
          // Clear it
          before = atomic_and(reinterpret_cast<T *>(&target), inv_mask);
          EXPECT_EQ(before & mask, mask);
          EXPECT_EQ(target.load() & mask, 0);
        }
      };

      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
      EXPECT_EQ(target.load(), 0);
    }
  }

  template <typename T>
  void CompareExchangeTest(const std::string &src) {
    // Setup the compilation environment
    sema::ErrorReporter error_reporter(&region_);
    ast::AstNodeFactory factory(&region_);
    ast::Context context(&region_, &error_reporter);

    // Compile it...
    auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
    auto module = compiler::Compiler::RunCompilationSimple(input);
    ASSERT_FALSE(module == nullptr);

    // The function should exist
    std::function<bool(T *, T *, T)> cmpxchg;
    EXPECT_TRUE(module->GetFunction("cmpxchg", vm::ExecutionMode::Interpret, &cmpxchg));

    /*=========================
     *= Run correctness tests =
     *=========================
     */

    std::default_random_engine generator;
    const uint32_t num_iters = 1000;
    const uint32_t num_threads = MultiThreadTestUtil::HardwareConcurrency();
    common::WorkerPool thread_pool(num_threads, {});

    for (uint32_t iter = 0; iter < num_iters; ++iter) {
      std::atomic<T> target = 0;
      auto workload = [&](T thread_id) {
        T expected;
        bool success = false;
        T previous = 0;
        do {
          expected = thread_id;
          EXPECT_FALSE(success);
          success = cmpxchg(reinterpret_cast<T *>(&target), &expected, thread_id + 1);
          EXPECT_LE(expected, thread_id);  // Out-of-order exchange occurred
          ASSERT_LE(previous, expected);   // Monotonic
          EXPECT_TRUE(!success || expected == thread_id);
          previous = expected;
        } while (expected != thread_id);
        EXPECT_TRUE(success);
      };

      MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
      ASSERT_EQ(target.load(), num_threads);
    }
  }
};

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicAndOr1) {
  AndOrTest<uint8_t>(R"(
    fun atomic_and(dest: *uint8, mask: uint8) -> uint8 {
      var x = @atomicAnd(dest, mask)
      return x
    }
    fun atomic_or(dest: *uint8, mask: uint8) -> uint8 {
      var x = @atomicOr(dest, mask)
      return x
    })");
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicAndOr2) {
  AndOrTest<uint16_t>(R"(
    fun atomic_and(dest: *uint16, mask: uint16) -> uint16 {
      var x = @atomicAnd(dest, mask)
      return x
    }
    fun atomic_or(dest: *uint16, mask: uint16) -> uint16 {
      var x = @atomicOr(dest, mask)
      return x
    })");
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicAndOr4) {
  AndOrTest<uint32_t>(R"(
    fun atomic_and(dest: *uint32, mask: uint32) -> uint32 {
      var x = @atomicAnd(dest, mask)
      return x
    }
    fun atomic_or(dest: *uint32, mask: uint32) -> uint32 {
      var x = @atomicOr(dest, mask)
      return x
    })");
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicAndOr8) {
  AndOrTest<uint64_t>(R"(
    fun atomic_and(dest: *uint64, mask: uint64) -> uint64 {
      var x = @atomicAnd(dest, mask)
      return x
    }
    fun atomic_or(dest: *uint64, mask: uint64) -> uint64 {
      var x = @atomicOr(dest, mask)
      return x
    })");
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicCompareExchange1) {
  CompareExchangeTest<uint8_t>(R"(
    fun cmpxchg(dest: *uint8, expected: *uint8, desired: uint8) -> bool {
      var x = @atomicCompareExchange(dest, expected, desired)
      return x
    })");
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicCompareExchange2) {
  CompareExchangeTest<uint16_t>(R"(
    fun cmpxchg(dest: *uint16, expected: *uint16, desired: uint16) -> bool {
      var x = @atomicCompareExchange(dest, expected, desired)
      return x
    })");
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicCompareExchange4) {
  CompareExchangeTest<uint32_t>(R"(
    fun cmpxchg(dest: *uint32, expected: *uint32, desired: uint32) -> bool {
      var x = @atomicCompareExchange(dest, expected, desired)
      return x
    })");
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicCompareExchange8) {
  CompareExchangeTest<uint64_t>(R"(
    fun cmpxchg(dest: *uint64, expected: *uint64, desired: uint64) -> bool {
      var x = @atomicCompareExchange(dest, expected, desired)
      return x
    })");
}
}  // namespace noisepage::execution::test
