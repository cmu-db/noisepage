#pragma once

#include <functional>
#include <string>
#include <utility>
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
  /**
   * Constructor
   * TODO(Amadou): Ask Prashant what this optimal parameter is for.
   * @param module bytecode module of the executing file
   * @param action_names names of the actions to take
   * @param optimal unused for now
   */
  MultiArmedBandit(vm::Module *module, std::vector<std::string> action_names, u32 optimal = 0)
      : module_(module), action_names_(std::move(action_names)) {}

  /**
   * Executes a given action
   * @param action to execute
   * @return reward of the action
   */
  double ExecuteAction(u32 action);

  /**
   * Translates execution time to reward.
   * @param time execution time
   * @return reward
   */
  static double ExecutionTimeToReward(double time);

  /**
   * Translates reward to execution time.
   * @param reward to translate
   * @return execution time
   */
  static double RewardToExecutionTime(double reward);

 private:
  // Not owned. It's is the responsibility of the user to make sure that this
  // is not deleted.
  vm::Module *module_;

  // The names of the  actions.
  std::vector<std::string> action_names_;
};

}  // namespace tpl::bandit
