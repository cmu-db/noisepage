#pragma once

#include <functional>
#include <string>

#include "common/managed_pointer.h"

namespace llvm {
class Module;
class TargetMachine;

namespace legacy {
class FunctionPassManager;
}  // namespace legacy
}  // namespace llvm

namespace noisepage::execution::vm {

class LLVMEngineCompilerOptions;

/** Metadata for each function. */
struct FunctionMetadata {
  std::string ir_;        ///< The IR of the function.
  uint64_t inst_count_;   ///< The instruction count of the function.
  uint64_t optimize_ns_;  ///< Time taken to optimize the function.
  uint64_t exec_ns_;      ///< Time taken to run the function.
};

/**
 * A mockup of information that we hope to obtain through Kyle's implementation of Tagged Dictionaries from
 * http://db.in.tum.de/~beischl/papers/Profiling_Dataflow_Systems_on_Multiple_Abstraction_Levels.pdf
 * lorem ipsum
 *
 * TODO(WAN): I guess in the absence of profile information it
 */
class FunctionProfile {
 public:
  FunctionProfile() = default;

  std::unordered_map<std::string, FunctionMetadata> functions_;
  std::vector<std::string> steps_;
  std::vector<std::string> teardowns_;
};

struct FunctionTransform {
  std::string name_;
  std::function<void(llvm::legacy::FunctionPassManager &)> transform_;
  // Any other metadata...
};

/**
 * Integration work where you decide how functions are represented, how they get applied, costing,
 * maybe connect this to metrics
 *
 * tpl.cpp
 *
 */
class FunctionOptimizer {
 public:
  explicit FunctionOptimizer(common::ManagedPointer<llvm::TargetMachine> target_machine);

  void Simplify(common::ManagedPointer<llvm::Module> llvm_module, const LLVMEngineCompilerOptions &options,
                common::ManagedPointer<FunctionProfile> profile);

  void Optimize(common::ManagedPointer<llvm::Module> llvm_module, const LLVMEngineCompilerOptions &options,
                common::ManagedPointer<FunctionProfile> profile);

 private:
  void FinalizeStats(common::ManagedPointer<llvm::Module> llvm_module, const LLVMEngineCompilerOptions &options,
                     common::ManagedPointer<FunctionProfile> profile);

  static FunctionTransform transforms[];

  const common::ManagedPointer<llvm::TargetMachine> target_machine_;
};

}  // namespace noisepage::execution::vm