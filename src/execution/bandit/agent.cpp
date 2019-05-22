#include "execution/bandit/agent.h"

namespace tpl::bandit {

void Agent::Reset() {
  value_estimates_.clear();
  action_attempts_.clear();
  value_estimates_.resize(num_actions_, prior_);
  action_attempts_.resize(num_actions_, 0);
  last_action_ = -1;
  timestep_ = 0;
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
  timestep_++;
}

}  // namespace tpl::bandit
