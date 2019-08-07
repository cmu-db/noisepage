#pragma once

#include <vector>

#include "execution/bandit/agent.h"
#include "execution/bandit/multi_armed_bandit.h"
#include "execution/util/common.h"

namespace terrier::execution::bandit {

/**
 * The environment for running the multi-armed bandit.
 */
class Environment {
 public:
  /**
   * Constructor
   * @param bandit the running multi-armed bandit.
   * @param agent agent managing the state.
   */
  Environment(MultiArmedBandit *bandit, Agent *agent) : bandit_(bandit), agent_(agent) {}

  /**
   * Reset the state of the environment.
   */
  void Reset();

  /**
   * Run the multi-armed bandit for num_trials on different partitions of the
   * table.
   * The reward obtained after each action is returned in rewards.
   * The action chosen is returned in actions.
   * If shuffle is set to true, then the partitions to execute an action is
   * chosen randomly without replacement. Else, it is chosen from 0 to
   * num_trials.
   */
  void Run(u32 num_trials, std::vector<double> *rewards, std::vector<u32> *actions, bool shuffle = false);

 private:
  // Bandit that executes the actions.
  MultiArmedBandit *bandit_;

  // The agent who manages the state.
  Agent *agent_;
};

}  // namespace terrier::execution::bandit
