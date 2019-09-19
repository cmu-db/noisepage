#include "execution/bandit/policy.h"

#include <algorithm>
#include <cfloat>
#include <chrono>  // NOLINT
#include <cmath>
#include <limits>
#include <vector>

#include "execution/bandit/agent.h"

#define MAX_EXPLORATION_VALUE 1000

namespace terrier::execution::bandit {

Policy::Policy(Kind kind)
    : kind_(kind), generator_(std::chrono::high_resolution_clock::now().time_since_epoch().count()) {}

namespace {

/**
 * Return the index of the maximum value. If multiple values are tied for the
 * maximum, then the index of a random value from that subset is returned.
 */
uint32_t ChooseBestIndex(const std::vector<double> &values, std::mt19937 *const generator) {
  const double max_value = *std::max_element(values.begin(), values.end());
  std::vector<uint32_t> best_indices{};

  for (uint32_t i = 0; i < values.size(); ++i) {
    if (std::fabs(values[i] - max_value) <= std::numeric_limits<double>::epsilon()) {
      best_indices.push_back(i);
    }
  }

  if (best_indices.size() == 1) {
    return best_indices[0];
  }
  return best_indices[(*generator)() % best_indices.size()];
}

}  // namespace

// With probability 1 - epsilon choose the action with the best value estimate.
// Otherwise choose a random action.
uint32_t EpsilonGreedyPolicy::NextAction(Agent *const agent) {
  const auto &value_estimates = agent->ValueEstimates();
  if (real_(generator_) < epsilon_) {
    return static_cast<uint32_t>(generator_() % value_estimates.size());
  }
  return ChooseBestIndex(value_estimates, &generator_);
}

// Choose the policy that maximises
//                                        ln(timesteps)
// (value_estimate + c * sqrt( ------------------------------------ ) )
//                             number of times the action was taken
//
// where c is a hyperparamter.
// If an action was never taken, it gets infinite preference.
uint32_t UCBPolicy::NextAction(Agent *const agent) {
  const auto &value_estimates = agent->ValueEstimates();
  const auto &action_attempts = agent->ActionAttempts();

  std::vector<double> values(action_attempts.size(), 0.0);
  std::vector<uint32_t> best_actions;

  for (uint32_t i = 0; i < action_attempts.size(); ++i) {
    double exploration = action_attempts[i] == 0 ? MAX_EXPLORATION_VALUE
                                                 : std::sqrt((std::log(agent->TimeStep() + 1) / action_attempts[i]));
    values[i] = value_estimates[i] + c_ * exploration;
  }

  return ChooseBestIndex(values, &generator_);
}

uint32_t AnnealingEpsilonGreedyPolicy::NextAction(Agent *const agent) {
  // Compute a new, decayed epsilon
  const auto &attempts = agent->ActionAttempts();
  const auto t = 1 + std::accumulate(attempts.begin(), attempts.end(), 0u);
  const auto new_epsilon = 1.0 / std::log(t + 0.0000001);
  SetEpsilon(new_epsilon);

  // Invoke epsilon-greedy policy with updated epsilon
  return EpsilonGreedyPolicy::NextAction(agent);
}

}  // namespace terrier::execution::bandit
