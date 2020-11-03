#include <random>
#include <string>

#include "common/worker_pool.h"
#include "execution/ast/context.h"
#include "execution/compiler/compiler.h"
#include "execution/sema/error_reporter.h"
#include "execution/tpl_test.h"
#include "execution/util/region.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"
#include "execution/vm/vm_defs.h"
#include "spdlog/fmt/fmt.h"
#include "test_util/multithread_test_util.h"

namespace noisepage::execution::test {

class AtomicsTest : public TplTest {
 public:
  AtomicsTest() : region_("atomics_test") { vm::LLVMEngine::Initialize(); }

  util::Region region_;

  template <typename T>
  void AndOrTest(const std::string &tpl_type, const bool compiled) {
    auto exec_mode = compiled ? vm::ExecutionMode::Compiled : vm::ExecutionMode::Interpret;
    // Setup the compilation environment
    sema::ErrorReporter error_reporter(&region_);
    ast::Context context(&region_, &error_reporter);

    const std::string &src = fmt::format(R"(
    fun atomic_and(dest: *{0}, mask: {0}) -> {0} {{
      var x = @atomicAnd(dest, mask)
      return x
    }}
    fun atomic_or(dest: *{0}, mask: {0}) -> {0} {{
      var x = @atomicOr(dest, mask)
      return x
    }})",
                                         tpl_type);

    // Compile it...
    auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
    auto module = compiler::Compiler::RunCompilationSimple(input);
    ASSERT_FALSE(module == nullptr);

    // The function should exist
    std::function<T(T *, T)> atomic_and;
    EXPECT_TRUE(module->GetFunction("atomic_and", exec_mode, &atomic_and));

    // The function should exist
    std::function<T(T *, T)> atomic_or;
    EXPECT_TRUE(module->GetFunction("atomic_or", exec_mode, &atomic_or));

    /*=========================
     *= Run correctness tests =
     *=========================
     */
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
  void CompareExchangeTest(const std::string &tpl_type, const bool compiled) {
    auto exec_mode = compiled ? vm::ExecutionMode::Compiled : vm::ExecutionMode::Interpret;
    // Setup the compilation environment
    sema::ErrorReporter error_reporter(&region_);
    ast::Context context(&region_, &error_reporter);

    const std::string &src = fmt::format(R"(
    fun cmpxchg(dest: *{0}, expected: *{0}, desired: {0}) -> bool {{
      var x = @atomicCompareExchange(dest, expected, desired)
      return x
    }})",
                                         tpl_type);

    // Compile it...
    auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
    auto module = compiler::Compiler::RunCompilationSimple(input);
    ASSERT_FALSE(module == nullptr);

    // The function should exist
    std::function<bool(T *, T *, T)> cmpxchg;
    EXPECT_TRUE(module->GetFunction("cmpxchg", exec_mode, &cmpxchg));

    /*=========================
     *= Run correctness tests =
     *=========================
     */
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

TEST_F(AtomicsTest, InterpretedAndOr1) { AndOrTest<uint8_t>("uint8", false); }                        // NOLINT
TEST_F(AtomicsTest, InterpretedAndOr2) { AndOrTest<uint16_t>("uint16", false); }                      // NOLINT
TEST_F(AtomicsTest, InterpretedAndOr4) { AndOrTest<uint32_t>("uint32", false); }                      // NOLINT
TEST_F(AtomicsTest, InterpretedAndOr8) { AndOrTest<uint64_t>("uint64", false); }                      // NOLINT
TEST_F(AtomicsTest, InterpretedCompareExchange1) { CompareExchangeTest<uint8_t>("uint8", false); }    // NOLINT
TEST_F(AtomicsTest, InterpretedCompareExchange2) { CompareExchangeTest<uint16_t>("uint16", false); }  // NOLINT
TEST_F(AtomicsTest, InterpretedCompareExchange4) { CompareExchangeTest<uint32_t>("uint32", false); }  // NOLINT
TEST_F(AtomicsTest, InterpretedCompareExchange8) { CompareExchangeTest<uint64_t>("uint64", false); }  // NOLINT

/* TODO(John): These tests are disabled because our testing environment does not
 *  currently make `bytecode_handlers_ir.bc` available to the test programs.
 *  Without this file, compiled tests fail.
 */
// TEST_F(AtomicsTest, CompiledAndOr1) { AndOrTest<uint8_t>("uint8", true); }                        // NOLINT
// TEST_F(AtomicsTest, CompiledAndOr2) { AndOrTest<uint16_t>("uint16", true); }                      // NOLINT
// TEST_F(AtomicsTest, CompiledAndOr4) { AndOrTest<uint32_t>("uint32", true); }                      // NOLINT
// TEST_F(AtomicsTest, CompiledAndOr8) { AndOrTest<uint64_t>("uint64", true); }                      // NOLINT
// TEST_F(AtomicsTest, CompiledCompareExchange1) { CompareExchangeTest<uint8_t>("uint8", true); }    // NOLINT
// TEST_F(AtomicsTest, CompiledCompareExchange2) { CompareExchangeTest<uint16_t>("uint16", true); }  // NOLINT
// TEST_F(AtomicsTest, CompiledCompareExchange4) { CompareExchangeTest<uint32_t>("uint32", true); }  // NOLINT
// TEST_F(AtomicsTest, CompiledCompareExchange8) { CompareExchangeTest<uint64_t>("uint64", true); }  // NOLINT

}  // namespace noisepage::execution::test
