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

  std::string ToStrShort() const;
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
  enum class Strategy {
    NOOP,
    PMENON,
  };

  struct MetadataAgg {
    uint64_t num_samples_;
    FunctionMetadata min_;
    FunctionMetadata mean_;
    FunctionMetadata max_;
  };

  FunctionProfile() = default;

  void SetStrategy(Strategy strategy) { strategy_ = strategy; }
  Strategy GetStrategy(Strategy strategy) const { return strategy_; }

  void StartAgg();
  void StopAgg() { should_update_agg_ = false; }
  bool IsAgg() const { return should_update_agg_; }

  void SetNumIterationsLeft(uint64_t num_iterations_left) { num_iterations_left_ = num_iterations_left; }
  void EndIteration();

  void RegisterSteps(const std::vector<std::string> &steps) { steps_ = steps; }
  void RegisterTeardowns(const std::vector<std::string> &teardowns) { teardowns_ = teardowns; }
  const std::vector<std::string> &GetSteps() const { return steps_; }
  const std::vector<std::string> &GetTeardowns() const { return teardowns_; }

  common::ManagedPointer<FunctionMetadata> GetPrev(const std::string &func_name) {
    return common::ManagedPointer(&functions_[func_name].prev_);
  }
  common::ManagedPointer<FunctionMetadata> GetCurr(const std::string &func_name) {
    return common::ManagedPointer(&functions_[func_name].curr_);
  }
  common::ManagedPointer<MetadataAgg> GetAgg(const std::string &func_name) {
    return common::ManagedPointer(&functions_[func_name].agg_);
  }

  FunctionMetadata GetCombinedPrev() const;
  const MetadataAgg &GetCombinedAgg() const { return combined_agg_; }

 private:
  Strategy strategy_;

  uint64_t num_iterations_left_;  ///< When this reaches 0, there are no more profiling iterations coming. Last chance.
  std::vector<std::string> steps_;
  std::vector<std::string> teardowns_;

  struct MetadataPair {
    FunctionMetadata prev_;
    FunctionMetadata curr_;
    MetadataAgg agg_;
  };

  std::unordered_map<std::string, MetadataPair> functions_;
  MetadataAgg combined_agg_;
  bool should_update_agg_{false};
  bool is_agg_initialized_{false};
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