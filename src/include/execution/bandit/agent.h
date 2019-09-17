#pragma once

#include <vector>

#include "execution/bandit/policy.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::bandit {

/**
 * An Agent is able to take one of a set of actions at each time step. The
 * action is chosen using a strategy based on the history of prior actions
 * and outcome observations.
 */
class Agent {
 public:
  /**
   * Constructor
   * @param policy policy to use by the agent
   * @param num_actions total number of actions
   * @param prior prior value estimate of an action
   * @param gamma hyper-parameter to handle dynamic distributions
   */
  Agent(Policy *policy, uint32_t num_actions, double prior = 0, double gamma = -1);

  /**
   * Reset all state this agent has collected.
   */
  void Reset();

  /**
   * Return the next action to be taken.
   */
  uint32_t NextAction();

  /**
   * Update the state based on reward obtained by playing the action chosen
   * earlier.
   */
  void Observe(double reward);

  /**
   * Return the current optimal action.
   */
  uint32_t GetCurrentOptimalAction() const;

  /**
   * Return estimations of the value of each flavor/action
   */
  const std::vector<double> &ValueEstimates() const { return value_estimates_; }

  /**
   * Return counts of the number of times each flavor/action was tried
   */
  const std::vector<uint32_t> &ActionAttempts() const { return action_attempts_; }

  /**
   * Return the current time step
   */
  uint32_t TimeStep() const { return time_step_; }

 private:
  // Policy to use for choosing the next action.
  Policy *policy_;

  // Number of actions available.
  uint32_t num_actions_;

  // Prior value estimate of an action. This is the same for every action.
  double prior_;

  // Hyper-parameter to handle dynamic distributions. By default, this is set to
  // 1 / (number of times an action was taken). This equally weights every
  // reward obtained for the actions in the entire history. This can be set to
  // a constant in [0, 1) to weigh recent rewards more than older ones.
  double gamma_;

  // Estimated value for each action.
  std::vector<double> value_estimates_;

  // Number of times each action was taken.
  std::vector<uint32_t> action_attempts_;

  // The current time step in the run.
  uint32_t time_step_;

  // Last action that was taken.
  uint32_t last_action_;
};

}  // namespace terrier::execution::bandit
