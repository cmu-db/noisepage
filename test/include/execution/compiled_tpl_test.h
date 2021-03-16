#pragma once

#include <memory>
#include <utility>

#include "execution/tpl_test.h"
#include "execution/vm/llvm_engine.h"
#include "test_util/fs_util.h"

namespace noisepage::execution {
/**
 * CompiledTplTest encapsulates the functionality necessary to run unit tests
 * in compiled (or adaptive) execution modes. Namely, it locates the bytecode
 * handlers file used internally by the LLVM engine during compiled execution
 * and makes it available to the test suite.
 */
class CompiledTplTest : public TplTest {
 public:
  /**
   * Perform suite-level initialization by resolving the path to the bytecode handlers file.
   * This function is invoked once per test suite by the test suite runner.
   */
  static void SetUpTestSuite() {
    const auto bytecode_handlers_path = common::GetBinaryArtifactPath("bytecode_handlers_ir.bc");
    auto settings = std::make_unique<const typename vm::LLVMEngine::Settings>(bytecode_handlers_path);
    vm::LLVMEngine::Initialize(std::move(settings));
  }

  /**
   * Perform suite-level teardown by releasing the bytecode handlers path.
   * This function is invoked once per test suite by the test suite runner.
   */
  static void TearDownTestSuite() { vm::LLVMEngine::Shutdown(); }
};
}  // namespace noisepage::execution
