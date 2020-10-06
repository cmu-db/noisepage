#pragma once

#include <llvm/Support/ManagedStatic.h>
#include <llvm/Support/TargetSelect.h>

namespace terrier::common {

class LLVM {
 public:
  static const unsigned DEFAULT_ADDRESS_SPACE = 0;

  static void Initialize() {
    // Global LLVM initialization
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
  }

  static void Shutdown() { llvm::llvm_shutdown(); }
};
}  // namespace terrier::common
