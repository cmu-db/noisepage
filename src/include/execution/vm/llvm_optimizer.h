#pragma once

#include "common/managed_pointer.h"
#include "execution/vm/llvm_engine.h"

namespace llvm {
class Module;
class TargetMachine;

namespace legacy {
class FunctionPassManager;
}  // namespace legacy
}  // namespace llvm

namespace noisepage::execution::vm {

/**
 * A mockup of information that we hope to obtain through Kyle's implementation of Tagged Dictionaries from
 * http://db.in.tum.de/~beischl/papers/Profiling_Dataflow_Systems_on_Multiple_Abstraction_Levels.pdf
 * lorem ipsum
 *
 * TODO(WAN): I guess in the absence of profile information it
 */
class ProfileInformation {
 public:
  ProfileInformation() = default;

 private:
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

  void Simplify(common::ManagedPointer<llvm::Module> llvm_module, common::ManagedPointer<ProfileInformation> profile,
                common::ManagedPointer<const LLVMEngine::Settings> engine_settings);

  void Optimize(common::ManagedPointer<llvm::Module> llvm_module, common::ManagedPointer<ProfileInformation> profile,
                common::ManagedPointer<const LLVMEngine::Settings> engine_settings);

 private:
  static FunctionTransform transforms[];

  const common::ManagedPointer<llvm::TargetMachine> target_machine_;
};

}  // namespace noisepage::execution::vm