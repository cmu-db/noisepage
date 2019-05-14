#pragma once

#include <functional>
#include <string>
#include <vector>

#include "execution/util/common.h"
#include "execution/util/macros.h"
#include "execution/vm/bytecode_module.h"

namespace tpl::bandit {

/**
 * A multi-armed bandit that executes an action and returns the reward.
 */
class MultiArmedBandit {
 public:
  MultiArmedBandit(vm::BytecodeModule *module,
                   std::vector<std::string> &&action_names, u32 optimal = 0)
      : module_(module), action_names_(std::move(action_names)) {}

  double ExecuteAction(u32 action);

  /**
   * Translates execution time to reward.
   */
  static double ExecutionTimeToReward(double time);

  /**
   * Translates reward to execution time.
   */
  static double RewardToExecutionTime(double reward);

 private:
  // TODO(siva): Clean up the members of the class.
  // Not owned. It's is the responsibility of the user to make sure that this
  // is not deleted.
  vm::BytecodeModule *module_;

  // The names of the  actions.
  std::vector<std::string> action_names_;
};

}  // namespace tpl::bandit
