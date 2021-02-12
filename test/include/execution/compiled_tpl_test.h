#pragma once

#include "execution/tpl_test.h"
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
    bytecode_handlers_path =
        std::make_unique<std::string>(common::FindFileFrom("bytecode_handlers_ir.bc", common::GetProjectRootPath()));
  }

  /**
   * Perform suite-level teardown by releasing the bytecode handlers path.
   * This function is invoked once per test suite by the test suite runner.
   */
  static void TearDownTestSuite() { bytecode_handlers_path.reset(); }

  /**
   * @return The path to the bytecode handlers bitcode file.
   */
  static const std::string &GetBytecodeHandlersPath() { return *bytecode_handlers_path; }

 private:
  /**
   * The path to the bytecode handlers bitcode file; this is
   * dynamically resolved once for the entire test suite.
   */
  static inline std::unique_ptr<std::string> bytecode_handlers_path{};  // NOLINT
};
}  // namespace noisepage::execution