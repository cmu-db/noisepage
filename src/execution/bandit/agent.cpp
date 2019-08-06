#include "execution/bandit/agent.h"

#include <algorithm>

namespace terrier::bandit {

Agent::Agent(Policy *policy, u32 num_actions, double prior, double gamma)
    : policy_(policy), num_actions_(num_actions), prior_(prior), gamma_(gamma) {
  Reset();
}

void Agent::Reset() {
  value_estimates_.clear();
  action_attempts_.clear();
  value_estimates_.resize(num_actions_, prior_);
  action_attempts_.resize(num_actions_, 0);
  last_action_ = -1;
  time_step_ = 0;
}

u32 Agent::NextAction() {
  u32 action = policy_->NextAction(this);
  last_action_ = action;
  return action;
}

void Agent::Observe(double reward) {
  action_attempts_[last_action_]++;

  double g;
  if (gamma_ < 0) {
    g = 1.0 / action_attempts_[last_action_];
  } else {
    g = gamma_;
  }

  value_estimates_[last_action_] += g * (reward - value_estimates_[last_action_]);
  time_step_++;
}

u32 Agent::GetCurrentOptimalAction() const {
  auto iter_max = std::max_element(value_estimates_.begin(), value_estimates_.end());
  return static_cast<u32>(std::distance(value_estimates_.begin(), iter_max));
}

}  // namespace terrier::bandit
