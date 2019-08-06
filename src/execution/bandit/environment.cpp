#include "execution/bandit/environment.h"

#include <algorithm>
#include <cstdlib>
#include <vector>

#include "execution/bandit/agent.h"
#include "execution/bandit/multi_armed_bandit.h"

u32 current_partition;

namespace terrier::bandit {

void Environment::Reset() { agent_->Reset(); }

void Environment::Run(u32 num_trials, std::vector<double> *rewards, std::vector<u32> *actions, bool shuffle) {
  rewards->resize(num_trials, 0);
  actions->resize(num_trials, 0);

  // Each trial is done on a different partition.
  std::vector<u32> partitions(num_trials);
  for (u32 i = 0; i < num_trials; i++) {
    partitions[i] = i;
  }

  std::random_device rd;
  std::mt19937 g(rd());

  if (shuffle) {
    std::shuffle(partitions.begin(), partitions.end(), g);
  }

  for (u32 i = 0; i < num_trials; ++i) {
    // TODO(siva): Hack! Fix me!
    current_partition = partitions[i];
    auto action = agent_->NextAction();
    auto reward = bandit_->ExecuteAction(action);
    agent_->Observe(reward);
    (*rewards)[i] = reward;
    (*actions)[i] = action;
  }
}

}  // namespace terrier::bandit
