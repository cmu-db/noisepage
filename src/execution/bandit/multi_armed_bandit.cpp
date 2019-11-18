#include "execution/bandit/multi_armed_bandit.h"

#include "common/timer.h"
#include "execution/vm/module.h"

namespace terrier::execution::bandit {

double MultiArmedBandit::ExecutionTimeToReward(double time) {
  // TODO(siva): Fix me!
  return (8 - time) / 2.5;
}

double MultiArmedBandit::RewardToExecutionTime(double reward) {
  // TODO(siva): Fix me!
  return 8 - 2.5 * reward;
}

double MultiArmedBandit::ExecuteAction(uint32_t action) {
  double exec_ms = 0;
  {
    common::ScopedTimer<std::chrono::milliseconds> timer(&exec_ms);

    // TODO(siva): Templatize this.
    std::function<uint32_t()> f;
    if (!module_->GetFunction(action_names_[action], vm::ExecutionMode::Interpret, &f)) {
      EXECUTION_LOG_ERROR("No {}() entry function found", action_names_[action]);
      return -1.0;
    }

    // Execute the action.
    f();
  }

  return ExecutionTimeToReward(exec_ms);
}

}  // namespace terrier::execution::bandit
