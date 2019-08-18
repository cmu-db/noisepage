#include "execution/bandit/environment.h"

#include <algorithm>
#include <cstdlib>
#include <vector>

#include "execution/bandit/agent.h"
#include "execution/bandit/multi_armed_bandit.h"

uint32_t current_partition;

namespace terrier::execution::bandit {

void Environment::Reset() { agent_->Reset(); }

void Environment::Run(uint32_t num_trials, std::vector<double> *rewards, std::vector<uint32_t> *actions, bool shuffle) {
  rewards->resize(num_trials, 0);
  actions->resize(num_trials, 0);

  // Each trial is done on a different partition.
  std::vector<uint32_t> partitions(num_trials);
  for (uint32_t i = 0; i < num_trials; i++) {
    partitions[i] = i;
  }

  std::random_device rd;
  std::mt19937 g(rd());

  if (shuffle) {
    std::shuffle(partitions.begin(), partitions.end(), g);
  }

  for (uint32_t i = 0; i < num_trials; ++i) {
    // TODO(siva): Hack! Fix me!
    current_partition = partitions[i];
    auto action = agent_->NextAction();
    auto reward = bandit_->ExecuteAction(action);
    agent_->Observe(reward);
    (*rewards)[i] = reward;
    (*actions)[i] = action;
  }
}

}  // namespace terrier::execution::bandit
