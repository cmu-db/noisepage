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
  compiler::CodeGen codegen(&context, /* CatalogAccessor */ DISABLED);

  // Declare the function using the function builder...
  auto operand_kind = ast::BuiltinType::Kind::Uint8;

  // Build the AND function...
  ast::FieldDecl *dest =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *operand =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("operand"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params_and(2, nullptr, &region_);
  params_and[0] = dest;
  params_and[1] = operand;
  compiler::FunctionBuilder fb_and(&codegen, codegen.MakeFreshIdentifier("atomic_and"), std::move(params_and),
                                   codegen.Nil());
  fb_and.Append(codegen.CallBuiltin(ast::Builtin::AtomicAnd1,
                                    {fb_and.GetParameterByPosition(0), fb_and.GetParameterByPosition(1)}));
  fb_and.Finish();
  auto and_decl = fb_and.GetConstructedFunction();

  // Build the OR function...
  dest = factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  operand = factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("operand"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params_or(2, nullptr, &region_);
  params_or[0] = dest;
  params_or[1] = operand;
  compiler::FunctionBuilder fb_or(&codegen, codegen.MakeFreshIdentifier("atomic_or"), std::move(params_or),
                                  codegen.Nil());
  fb_or.Append(
      codegen.CallBuiltin(ast::Builtin::AtomicOr1, {fb_or.GetParameterByPosition(0), fb_or.GetParameterByPosition(1)}));
  fb_or.Finish();
  auto or_decl = fb_or.GetConstructedFunction();

  // Create the "file"...
  util::RegionVector<ast::Decl *> fn_decls(2, nullptr, &region_);
  fn_decls[0] = and_decl;
  fn_decls[1] = or_decl;
  auto root_node = factory.NewFile(pos_, std::move(fn_decls));

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, root_node);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_FALSE(module == nullptr);

  // The function should exist
  std::function<void(uint8_t *, uint8_t)> atomic_and;
  EXPECT_TRUE(module->GetFunction("atomic_and", vm::ExecutionMode::Interpret, &atomic_and));

  // The function should exist
  std::function<void(uint8_t *, uint8_t)> atomic_or;
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
      for (uint32_t i = 0; i < num_cycles; ++i) {
        // Set it
        atomic_or(reinterpret_cast<uint8_t *>(&target), mask);
        EXPECT_EQ(target.load() & mask, mask);
        // Clear it
        atomic_and(reinterpret_cast<uint8_t *>(&target), inv_mask);
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
  compiler::CodeGen codegen(&context, /* CatalogAccessor */ DISABLED);

  // Declare the function using the function builder...
  auto operand_kind = ast::BuiltinType::Kind::Uint16;

  // Build the AND function...
  ast::FieldDecl *dest =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *operand =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("operand"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params_and(2, nullptr, &region_);
  params_and[0] = dest;
  params_and[1] = operand;
  compiler::FunctionBuilder fb_and(&codegen, codegen.MakeFreshIdentifier("atomic_and"), std::move(params_and),
                                   codegen.Nil());
  fb_and.Append(codegen.CallBuiltin(ast::Builtin::AtomicAnd2,
                                    {fb_and.GetParameterByPosition(0), fb_and.GetParameterByPosition(1)}));
  fb_and.Finish();
  auto and_decl = fb_and.GetConstructedFunction();

  // Build the OR function...
  dest = factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  operand = factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("operand"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params_or(2, nullptr, &region_);
  params_or[0] = dest;
  params_or[1] = operand;
  compiler::FunctionBuilder fb_or(&codegen, codegen.MakeFreshIdentifier("atomic_or"), std::move(params_or),
                                  codegen.Nil());
  fb_or.Append(
      codegen.CallBuiltin(ast::Builtin::AtomicOr2, {fb_or.GetParameterByPosition(0), fb_or.GetParameterByPosition(1)}));
  fb_or.Finish();
  auto or_decl = fb_or.GetConstructedFunction();

  // Create the "file"...
  util::RegionVector<ast::Decl *> fn_decls(2, nullptr, &region_);
  fn_decls[0] = and_decl;
  fn_decls[1] = or_decl;
  auto root_node = factory.NewFile(pos_, std::move(fn_decls));

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, root_node);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_FALSE(module == nullptr);

  // The function should exist
  std::function<void(uint16_t *, uint16_t)> atomic_and;
  EXPECT_TRUE(module->GetFunction("atomic_and", vm::ExecutionMode::Interpret, &atomic_and));

  // The function should exist
  std::function<void(uint16_t *, uint16_t)> atomic_or;
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
      for (uint32_t i = 0; i < num_cycles; ++i) {
        // Set it
        atomic_or(reinterpret_cast<uint16_t *>(&target), mask);
        EXPECT_EQ(target.load() & mask, mask);
        // Clear it
        atomic_and(reinterpret_cast<uint16_t *>(&target), inv_mask);
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
  compiler::CodeGen codegen(&context, /* CatalogAccessor */ DISABLED);

  // Declare the function using the function builder...
  auto operand_kind = ast::BuiltinType::Kind::Uint32;

  // Build the AND function...
  ast::FieldDecl *dest =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *operand =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("operand"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params_and(2, nullptr, &region_);
  params_and[0] = dest;
  params_and[1] = operand;
  compiler::FunctionBuilder fb_and(&codegen, codegen.MakeFreshIdentifier("atomic_and"), std::move(params_and),
                                   codegen.Nil());
  fb_and.Append(codegen.CallBuiltin(ast::Builtin::AtomicAnd4,
                                    {fb_and.GetParameterByPosition(0), fb_and.GetParameterByPosition(1)}));
  fb_and.Finish();
  auto and_decl = fb_and.GetConstructedFunction();

  // Build the OR function...
  dest = factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  operand = factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("operand"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params_or(2, nullptr, &region_);
  params_or[0] = dest;
  params_or[1] = operand;
  compiler::FunctionBuilder fb_or(&codegen, codegen.MakeFreshIdentifier("atomic_or"), std::move(params_or),
                                  codegen.Nil());
  fb_or.Append(
      codegen.CallBuiltin(ast::Builtin::AtomicOr4, {fb_or.GetParameterByPosition(0), fb_or.GetParameterByPosition(1)}));
  fb_or.Finish();
  auto or_decl = fb_or.GetConstructedFunction();

  // Create the "file"...
  util::RegionVector<ast::Decl *> fn_decls(2, nullptr, &region_);
  fn_decls[0] = and_decl;
  fn_decls[1] = or_decl;
  auto root_node = factory.NewFile(pos_, std::move(fn_decls));

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, root_node);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_FALSE(module == nullptr);

  // The function should exist
  std::function<void(uint32_t *, uint32_t)> atomic_and;
  EXPECT_TRUE(module->GetFunction("atomic_and", vm::ExecutionMode::Interpret, &atomic_and));

  // The function should exist
  std::function<void(uint32_t *, uint32_t)> atomic_or;
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
      for (uint32_t i = 0; i < num_cycles; ++i) {
        // Set it
        atomic_or(reinterpret_cast<uint32_t *>(&target), mask);
        EXPECT_EQ(target.load() & mask, mask);
        // Clear it
        atomic_and(reinterpret_cast<uint32_t *>(&target), inv_mask);
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
  compiler::CodeGen codegen(&context, /* CatalogAccessor */ DISABLED);

  // Declare the function using the function builder...
  auto operand_kind = ast::BuiltinType::Kind::Uint64;

  // Build the AND function...
  ast::FieldDecl *dest =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *operand =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("operand"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params_and(2, nullptr, &region_);
  params_and[0] = dest;
  params_and[1] = operand;
  compiler::FunctionBuilder fb_and(&codegen, codegen.MakeFreshIdentifier("atomic_and"), std::move(params_and),
                                   codegen.Nil());
  fb_and.Append(codegen.CallBuiltin(ast::Builtin::AtomicAnd8,
                                    {fb_and.GetParameterByPosition(0), fb_and.GetParameterByPosition(1)}));
  fb_and.Finish();
  auto and_decl = fb_and.GetConstructedFunction();

  // Build the OR function...
  dest = factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  operand = factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("operand"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params_or(2, nullptr, &region_);
  params_or[0] = dest;
  params_or[1] = operand;
  compiler::FunctionBuilder fb_or(&codegen, codegen.MakeFreshIdentifier("atomic_or"), std::move(params_or),
                                  codegen.Nil());
  fb_or.Append(
      codegen.CallBuiltin(ast::Builtin::AtomicOr8, {fb_or.GetParameterByPosition(0), fb_or.GetParameterByPosition(1)}));
  fb_or.Finish();
  auto or_decl = fb_or.GetConstructedFunction();

  // Create the "file"...
  util::RegionVector<ast::Decl *> fn_decls(2, nullptr, &region_);
  fn_decls[0] = and_decl;
  fn_decls[1] = or_decl;
  auto root_node = factory.NewFile(pos_, std::move(fn_decls));

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, root_node);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_FALSE(module == nullptr);

  // The function should exist
  std::function<void(uint64_t *, uint64_t)> atomic_and;
  EXPECT_TRUE(module->GetFunction("atomic_and", vm::ExecutionMode::Interpret, &atomic_and));

  // The function should exist
  std::function<void(uint64_t *, uint64_t)> atomic_or;
  EXPECT_TRUE(module->GetFunction("atomic_or", vm::ExecutionMode::Interpret, &atomic_or));

  /*=========================
   *= Run correctness tests =
   *=========================
   */

  std::default_random_engine generator;
  const uint32_t num_iters = 100;
  const uint32_t num_cycles = 16400;
  const uint32_t num_threads = 8;  // Number of bits in test
  common::WorkerPool thread_pool(num_threads, {});

  for (uint32_t iter = 0; iter < num_iters; ++iter) {
    std::atomic<uint64_t> target = 0;
    auto workload = [&](uint32_t thread_id) {
      auto mask = 1 << thread_id;
      auto inv_mask = ~mask;
      ASSERT_NE(mask, 0);
      for (uint32_t i = 0; i < num_cycles; ++i) {
        // Set it
        atomic_or(reinterpret_cast<uint64_t *>(&target), mask);
        EXPECT_EQ(target.load() & mask, mask);
        // Clear it
        atomic_and(reinterpret_cast<uint64_t *>(&target), inv_mask);
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
  compiler::CodeGen codegen(&context, /* CatalogAccessor */ DISABLED);

  // Declare the function using the function builder...
  auto operand_kind = ast::BuiltinType::Kind::Uint8;

  // Build the function...
  ast::FieldDecl *dest =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *expected =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *desired =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("desired"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params(3, nullptr, &region_);
  params[0] = dest;
  params[1] = expected;
  params[2] = desired;
  compiler::FunctionBuilder fb(&codegen, codegen.MakeFreshIdentifier("cmpxchg"), std::move(params), codegen.Nil());
  fb.Append(
      codegen.CallBuiltin(ast::Builtin::AtomicCompareExchange1,
                          {fb.GetParameterByPosition(0), fb.GetParameterByPosition(1), fb.GetParameterByPosition(2)}));
  fb.Finish();
  auto cmpxchg_decl = fb.GetConstructedFunction();

  // Create the "file"...
  util::RegionVector<ast::Decl *> fn_decls(1, nullptr, &region_);
  fn_decls[0] = cmpxchg_decl;
  auto root_node = factory.NewFile(pos_, std::move(fn_decls));

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, root_node);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<void(uint8_t *, uint8_t *, uint8_t)> cmpxchg;
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
      do {
        expected = thread_id;
        cmpxchg(reinterpret_cast<uint8_t *>(&target), &expected, static_cast<uint8_t>(thread_id + 1));
        EXPECT_LE(expected, thread_id);  // Out-of-order exchange occurred
      } while (expected != thread_id);
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
  compiler::CodeGen codegen(&context, /* CatalogAccessor */ DISABLED);

  // Declare the function using the function builder...
  auto operand_kind = ast::BuiltinType::Kind::Uint16;

  // Build the function...
  ast::FieldDecl *dest =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *expected =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *desired =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("desired"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params(3, nullptr, &region_);
  params[0] = dest;
  params[1] = expected;
  params[2] = desired;
  compiler::FunctionBuilder fb(&codegen, codegen.MakeFreshIdentifier("cmpxchg"), std::move(params), codegen.Nil());
  fb.Append(
      codegen.CallBuiltin(ast::Builtin::AtomicCompareExchange2,
                          {fb.GetParameterByPosition(0), fb.GetParameterByPosition(1), fb.GetParameterByPosition(2)}));
  fb.Finish();
  auto cmpxchg_decl = fb.GetConstructedFunction();

  // Create the "file"...
  util::RegionVector<ast::Decl *> fn_decls(1, nullptr, &region_);
  fn_decls[0] = cmpxchg_decl;
  auto root_node = factory.NewFile(pos_, std::move(fn_decls));

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, root_node);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<void(uint16_t *, uint16_t *, uint16_t)> cmpxchg;
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
      do {
        expected = thread_id;
        cmpxchg(reinterpret_cast<uint16_t *>(&target), &expected, static_cast<uint16_t>(thread_id + 1));
        EXPECT_LE(expected, thread_id);  // Out-of-order exchange occurred
      } while (expected != thread_id);
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
  compiler::CodeGen codegen(&context, /* CatalogAccessor */ DISABLED);

  // Declare the function using the function builder...
  auto operand_kind = ast::BuiltinType::Kind::Uint32;

  // Build the function...
  ast::FieldDecl *dest =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *expected =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *desired =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("desired"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params(3, nullptr, &region_);
  params[0] = dest;
  params[1] = expected;
  params[2] = desired;
  compiler::FunctionBuilder fb(&codegen, codegen.MakeFreshIdentifier("cmpxchg"), std::move(params), codegen.Nil());
  fb.Append(
      codegen.CallBuiltin(ast::Builtin::AtomicCompareExchange4,
                          {fb.GetParameterByPosition(0), fb.GetParameterByPosition(1), fb.GetParameterByPosition(2)}));
  fb.Finish();
  auto cmpxchg_decl = fb.GetConstructedFunction();

  // Create the "file"...
  util::RegionVector<ast::Decl *> fn_decls(1, nullptr, &region_);
  fn_decls[0] = cmpxchg_decl;
  auto root_node = factory.NewFile(pos_, std::move(fn_decls));

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, root_node);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<void(uint32_t *, uint32_t *, uint32_t)> cmpxchg;
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
      do {
        expected = thread_id;
        cmpxchg(reinterpret_cast<uint32_t *>(&target), &expected, static_cast<uint32_t>(thread_id + 1));
        EXPECT_LE(expected, thread_id);  // Out-of-order exchange occurred
      } while (expected != thread_id);
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
  compiler::CodeGen codegen(&context, /* CatalogAccessor */ DISABLED);

  // Declare the function using the function builder...
  auto operand_kind = ast::BuiltinType::Kind::Uint64;

  // Build the function...
  ast::FieldDecl *dest =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *expected =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("dest"), codegen.PointerType(operand_kind));
  ast::FieldDecl *desired =
      factory.NewFieldDecl(pos_, codegen.MakeFreshIdentifier("desired"), codegen.BuiltinType(operand_kind));
  util::RegionVector<ast::FieldDecl *> params(3, nullptr, &region_);
  params[0] = dest;
  params[1] = expected;
  params[2] = desired;
  compiler::FunctionBuilder fb(&codegen, codegen.MakeFreshIdentifier("cmpxchg"), std::move(params), codegen.Nil());
  fb.Append(
      codegen.CallBuiltin(ast::Builtin::AtomicCompareExchange8,
                          {fb.GetParameterByPosition(0), fb.GetParameterByPosition(1), fb.GetParameterByPosition(2)}));
  fb.Finish();
  auto cmpxchg_decl = fb.GetConstructedFunction();

  // Create the "file"...
  util::RegionVector<ast::Decl *> fn_decls(1, nullptr, &region_);
  fn_decls[0] = cmpxchg_decl;
  auto root_node = factory.NewFile(pos_, std::move(fn_decls));

  // Compile it...
  auto input = compiler::Compiler::Input("Atomic Definitions", &context, root_node);
  auto module = compiler::Compiler::RunCompilationSimple(input);
  EXPECT_STREQ(error_reporter.SerializeErrors().c_str(), "");
  ASSERT_FALSE(module == nullptr);

  // The function should exist
  std::function<void(uint64_t *, uint64_t *, uint64_t)> cmpxchg;
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
      do {
        expected = thread_id;
        cmpxchg(reinterpret_cast<uint64_t *>(&target), &expected, thread_id + 1);
        EXPECT_LE(expected, thread_id);  // Out-of-order exchange occurred
      } while (expected != thread_id);
    };

    MultiThreadTestUtil::RunThreadsUntilFinish(&thread_pool, num_threads, workload);
    EXPECT_EQ(target.load(), num_threads);
  }
}
}  // namespace noisepage::execution::test
