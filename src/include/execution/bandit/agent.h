#pragma once

#include <vector>

#include "execution/bandit/policy.h"
#include "execution/util/common.h"

namespace tpl::bandit {

/**
 * An Agent is able to take one of a set of actions at each time step. The
 * action is chosen using a strategy based on the history of prior actions
 * and outcome observations.
 */
class Agent {
 public:
  /**
   * Constructor
   * @param policy policy to use
   * @param num_actions the number of actions
   * @param prior prior value estimate of actions
   * @param gamma hyperparameter to handle dynamic distrbutions
   */
  Agent(Policy *policy, u32 num_actions, double prior = 0, double gamma = -1)
      : policy_(policy), num_actions_(num_actions), prior_(prior), gamma_(gamma) {
    Reset();
  }

  /**
   * Resets all variables
   */
  void Reset();

  /**
   * @return the next action to be taken.
   */
  u32 NextAction();

  /**
   * Update the state based on reward obtained by playing the action chosen earlier.
   * @param reward reward obtained
   */
  void Observe(double reward);

  /**
   * @return the value estimates
   */
  const auto &value_estimates() { return value_estimates_; }

  /**
   * @return the action attempts
   */
  const auto &action_attempts() { return action_attempts_; }

  /**
   * @return the current timestep
   */
  auto timestep() { return timestep_; }

 private:
  // Policy to use for choosing the next action.
  Policy *policy_;

  // Number of actions available.
  u32 num_actions_;

  // Prior value estimate of an action. This is the same for every action.
  double prior_;

  // Hyperparameter to handle dynamic distrbutions. By default, this is set to
  // 1 / (number of times an action was taken). This equaly weights every
  // reward obtained for the actions in the entire history. This can be set to
  // a constant in [0, 1) to weigh recent rewards more than older ones.
  double gamma_;

  // Estimated value for each action.
  std::vector<double> value_estimates_;

  // Number of times each action was taken.
  std::vector<u32> action_attempts_;

  // The current time step in the run.
  u32 timestep_;

  // Last action that was taken.
  u32 last_action_;
};

}  // namespace tpl::bandit
