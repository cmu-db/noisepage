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
};

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicAndOr1) {
  // Setup the compilation environment
  sema::ErrorReporter error_reporter(&region_);
  ast::AstNodeFactory factory(&region_);
  ast::Context context(&region_, &error_reporter);

  auto src = std::string(
      "fun atomic_and(dest: *uint8, mask: uint8) -> uint8 {\n"
      "  var x = @atomicAnd(dest, mask)\n"
      "  return x\n"
      "}\n"
      "fun atomic_or(dest: *uint8, mask: uint8) -> uint8 {\n"
      "  var x = @atomicOr(dest, mask)\n"
      "  return x\n"
      "}");

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<uint8_t(uint8_t *, uint8_t)> atomic_and;
  EXPECT_TRUE(module->GetFunction("atomic_and", vm::ExecutionMode::Interpret, &atomic_and));

  // The function should exist
  std::function<uint8_t(uint8_t *, uint8_t)> atomic_or;
  EXPECT_TRUE(module->GetFunction("atomic_or", vm::ExecutionMode::Interpret, &atomic_or));

  /*=========================
   *= Run correctness tests =
   *=========================
   */

  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t num_cycles = 1000;
  const uint32_t num_threads = 8;  // Number of bits in test
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    std::atomic<uint8_t> target = 0;
    auto workload = [&](uint32_t thread_id) {
      auto mask = static_cast<uint8_t>(1 << thread_id);
      auto inv_mask = ~mask;
      ASSERT_NE(mask, 0);

      uint8_t before;
      for (uint32_t i = 0; i < num_cycles; ++i) {
        // Set it
        before = atomic_or(reinterpret_cast<uint8_t *>(&target), mask);
        EXPECT_EQ(before & mask, 0);
        EXPECT_EQ(target.load() & mask, mask);
        // Clear it
        before = atomic_and(reinterpret_cast<uint8_t *>(&target), inv_mask);
        EXPECT_EQ(before & mask, mask);
        EXPECT_EQ(target.load() & mask, 0);
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(target.load(), 0);
  }
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicAndOr2) {
  // Setup the compilation environment
  sema::ErrorReporter error_reporter(&region_);
  ast::AstNodeFactory factory(&region_);
  ast::Context context(&region_, &error_reporter);

  auto src = std::string(
      "fun atomic_and(dest: *uint16, mask: uint16) -> uint16 {\n"
      "  var x = @atomicAnd(dest, mask)\n"
      "  return x\n"
      "}\n"
      "fun atomic_or(dest: *uint16, mask: uint16) -> uint16 {\n"
      "  var x = @atomicOr(dest, mask)\n"
      "  return x\n"
      "}");

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<uint16_t(uint16_t *, uint16_t)> atomic_and;
  EXPECT_TRUE(module->GetFunction("atomic_and", vm::ExecutionMode::Interpret, &atomic_and));

  // The function should exist
  std::function<uint16_t(uint16_t *, uint16_t)> atomic_or;
  EXPECT_TRUE(module->GetFunction("atomic_or", vm::ExecutionMode::Interpret, &atomic_or));

  /*=========================
   *= Run correctness tests =
   *=========================
   */

  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t num_cycles = 1000;
  const uint32_t num_threads = 16;  // Number of bits in test
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    std::atomic<uint16_t> target = 0;
    auto workload = [&](uint32_t thread_id) {
      ASSERT_LT(thread_id, num_threads);
      auto mask = static_cast<uint16_t>(1 << thread_id);
      auto inv_mask = ~mask;
      ASSERT_NE(mask, 0);

      uint16_t before;
      for (uint32_t i = 0; i < num_cycles; ++i) {
        // Set it
        before = atomic_or(reinterpret_cast<uint16_t *>(&target), mask);
        EXPECT_EQ(before & mask, 0);
        EXPECT_EQ(target.load() & mask, mask);
        // Clear it
        before = atomic_and(reinterpret_cast<uint16_t *>(&target), inv_mask);
        EXPECT_EQ(before & mask, mask);
        EXPECT_EQ(target.load() & mask, 0);
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(target.load(), 0);
  }
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicAndOr4) {
  // Setup the compilation environment
  sema::ErrorReporter error_reporter(&region_);
  ast::AstNodeFactory factory(&region_);
  ast::Context context(&region_, &error_reporter);

  auto src = std::string(
      "fun atomic_and(dest: *uint32, mask: uint32) -> uint32 {\n"
      "  var x = @atomicAnd(dest, mask)\n"
      "  return x\n"
      "}\n"
      "fun atomic_or(dest: *uint32, mask: uint32) -> uint32 {\n"
      "  var x = @atomicOr(dest, mask)\n"
      "  return x\n"
      "}");

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<uint32_t(uint32_t *, uint32_t)> atomic_and;
  EXPECT_TRUE(module->GetFunction("atomic_and", vm::ExecutionMode::Interpret, &atomic_and));

  // The function should exist
  std::function<uint32_t(uint32_t *, uint32_t)> atomic_or;
  EXPECT_TRUE(module->GetFunction("atomic_or", vm::ExecutionMode::Interpret, &atomic_or));

  /*=========================
   *= Run correctness tests =
   *=========================
   */

  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t num_cycles = 1000;
  const uint32_t num_threads = 32;  // Number of bits in test
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    std::atomic<uint32_t> target = 0;
    auto workload = [&](uint32_t thread_id) {
      auto mask = static_cast<uint32_t>(1 << thread_id);
      auto inv_mask = ~mask;
      ASSERT_NE(mask, 0);

      uint32_t before;
      for (uint32_t i = 0; i < num_cycles; ++i) {
        // Set it
        before = atomic_or(reinterpret_cast<uint32_t *>(&target), mask);
        EXPECT_EQ(before & mask, 0);
        EXPECT_EQ(target.load() & mask, mask);
        // Clear it
        before = atomic_and(reinterpret_cast<uint32_t *>(&target), inv_mask);
        EXPECT_EQ(before & mask, mask);
        EXPECT_EQ(target.load() & mask, 0);
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(target.load(), 0);
  }
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicAndOr8) {
  // Setup the compilation environment
  sema::ErrorReporter error_reporter(&region_);
  ast::AstNodeFactory factory(&region_);
  ast::Context context(&region_, &error_reporter);

  auto src = std::string(
      "fun atomic_and(dest: *uint64, mask: uint64) -> uint64 {\n"
      "  var x = @atomicAnd(dest, mask)\n"
      "  return x\n"
      "}\n"
      "fun atomic_or(dest: *uint64, mask: uint64) -> uint64 {\n"
      "  var x = @atomicOr(dest, mask)\n"
      "  return x\n"
      "}");

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<uint64_t(uint64_t *, uint64_t)> atomic_and;
  EXPECT_TRUE(module->GetFunction("atomic_and", vm::ExecutionMode::Interpret, &atomic_and));

  // The function should exist
  std::function<uint64_t(uint64_t *, uint64_t)> atomic_or;
  EXPECT_TRUE(module->GetFunction("atomic_or", vm::ExecutionMode::Interpret, &atomic_or));

  /*=========================
   *= Run correctness tests =
   *=========================
   */

  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t num_cycles = 1000;
  const uint32_t num_threads = 64;  // Number of bits in test
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    std::atomic<uint64_t> target = 0;
    auto workload = [&](uint32_t thread_id) {
      auto mask = 1ull << thread_id;
      auto inv_mask = ~mask;
      ASSERT_NE(mask, 0);

      uint64_t before;
      for (uint32_t i = 0; i < num_cycles; ++i) {
        // Set it
        before = atomic_or(reinterpret_cast<uint64_t *>(&target), mask);
        ASSERT_EQ(before & mask, 0);
        EXPECT_EQ(target.load() & mask, mask);
        // Clear it
        before = atomic_and(reinterpret_cast<uint64_t *>(&target), inv_mask);
        EXPECT_EQ(before & mask, mask);
        EXPECT_EQ(target.load() & mask, 0);
      }
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(target.load(), 0);
  }
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicCompareExchange1) {
  // Setup the compilation environment
  sema::ErrorReporter error_reporter(&region_);
  ast::AstNodeFactory factory(&region_);
  ast::Context context(&region_, &error_reporter);

  auto src = std::string(
      "fun cmpxchg(dest: *uint8, expected: *uint8, desired: uint8) -> bool {"
      "  return @atomicCompareExchange(dest, expected, desired)"
      "}");

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<bool(uint8_t *, uint8_t *, uint8_t)> cmpxchg;
  EXPECT_TRUE(module->GetFunction("cmpxchg", vm::ExecutionMode::Interpret, &cmpxchg));

  /*=========================
   *= Run correctness tests =
   *=========================
   */

  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t num_threads = 8;  // Number of bits in test
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    std::atomic<uint8_t> target = 0;
    auto workload = [&](uint32_t thread_id) {
      uint8_t expected;
      bool success = false;
      do {
        expected = thread_id;
        EXPECT_FALSE(success);
        success = cmpxchg(reinterpret_cast<uint8_t *>(&target), &expected, static_cast<uint8_t>(thread_id + 1));
        EXPECT_LE(expected, thread_id);  // Out-of-order exchange occurred
        EXPECT_TRUE(!success || expected == thread_id);
      } while (expected != thread_id);
      EXPECT_TRUE(success);
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(target.load(), num_threads);
  }
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicCompareExchange2) {
  // Setup the compilation environment
  sema::ErrorReporter error_reporter(&region_);
  ast::AstNodeFactory factory(&region_);
  ast::Context context(&region_, &error_reporter);

  auto src = std::string(
      "fun cmpxchg(dest: *uint16, expected: *uint16, desired: uint16) -> bool {"
      "  return @atomicCompareExchange(dest, expected, desired)"
      "}");

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<bool(uint16_t *, uint16_t *, uint16_t)> cmpxchg;
  EXPECT_TRUE(module->GetFunction("cmpxchg", vm::ExecutionMode::Interpret, &cmpxchg));

  /*=========================
   *= Run correctness tests =
   *=========================
   */

  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t num_threads = 16;  // Number of bits in test
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    std::atomic<uint16_t> target = 0;
    auto workload = [&](uint32_t thread_id) {
      uint16_t expected;
      bool success = false;
      do {
        expected = thread_id;
        EXPECT_FALSE(success);
        success = cmpxchg(reinterpret_cast<uint16_t *>(&target), &expected, static_cast<uint16_t>(thread_id + 1));
        EXPECT_LE(expected, thread_id);  // Out-of-order exchange occurred
        EXPECT_TRUE(!success || expected == thread_id);
      } while (expected != thread_id);
      EXPECT_TRUE(success);
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(target.load(), num_threads);
  }
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicCompareExchange4) {
  // Setup the compilation environment
  sema::ErrorReporter error_reporter(&region_);
  ast::AstNodeFactory factory(&region_);
  ast::Context context(&region_, &error_reporter);

  auto src = std::string(
      "fun cmpxchg(dest: *uint32, expected: *uint32, desired: uint32) -> bool {"
      "  return @atomicCompareExchange(dest, expected, desired)"
      "}");

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<bool(uint32_t *, uint32_t *, uint32_t)> cmpxchg;
  EXPECT_TRUE(module->GetFunction("cmpxchg", vm::ExecutionMode::Interpret, &cmpxchg));

  /*=========================
   *= Run correctness tests =
   *=========================
   */

  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t num_threads = 32;  // Number of bits in test
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    std::atomic<uint32_t> target = 0;
    auto workload = [&](uint32_t thread_id) {
      uint32_t expected;
      bool success = false;
      do {
        expected = thread_id;
        EXPECT_FALSE(success);
        success = cmpxchg(reinterpret_cast<uint32_t *>(&target), &expected, static_cast<uint32_t>(thread_id + 1));
        EXPECT_LE(expected, thread_id);  // Out-of-order exchange occurred
        EXPECT_TRUE(!success || expected == thread_id);
      } while (expected != thread_id);
      EXPECT_TRUE(success);
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(target.load(), num_threads);
  }
}

// NOLINTNEXTLINE
TEST_F(AtomicsTest, AtomicCompareExchange8) {
  // Setup the compilation environment
  sema::ErrorReporter error_reporter(&region_);
  ast::AstNodeFactory factory(&region_);
  ast::Context context(&region_, &error_reporter);

  auto src = std::string(
      "fun cmpxchg(dest: *uint64, expected: *uint64, desired: uint64) -> bool {"
      "  return @atomicCompareExchange(dest, expected, desired)"
      "}");

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, &src);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<bool(uint64_t *, uint64_t *, uint64_t)> cmpxchg;
  EXPECT_TRUE(module->GetFunction("cmpxchg", vm::ExecutionMode::Interpret, &cmpxchg));

  /*=========================
   *= Run correctness tests =
   *=========================
   */

  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t num_threads = 64;  // Number of bits in test
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    std::atomic<uint64_t> target = 0;
    auto workload = [&](uint32_t thread_id) {
      uint64_t expected;
      bool success = false;
      do {
        expected = thread_id;
        EXPECT_FALSE(success);
        success = cmpxchg(reinterpret_cast<uint64_t *>(&target), &expected, thread_id + 1);
        EXPECT_LE(expected, thread_id);  // Out-of-order exchange occurred
        EXPECT_TRUE(!success || expected == thread_id);
      } while (expected != thread_id);
      EXPECT_TRUE(success);
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(target.load(), num_threads);
  }
}
}  // namespace noisepage::execution::test
