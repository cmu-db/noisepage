#pragma once
#include <atomic>

namespace terrier::common {

/**
 * All possible states of action.
 */
enum class ActionState {
  INITIATED = 0,
  IN_PROGRESS = 1,
  SUCCESS = 2,
  FAILURE = 3,
  DEFERRED = 4,
};

/**
 * ActionContext is used to keep track of action state and system behavior.
 */
class ActionContext {
 public:
  /**
   * Constructor of ActionContext.
   * @param action_id id of this action.
   */
  ActionContext(int32_t action_id) : action_id_(action_id) {}

  /**
   * Get the state of this action.
   * @return action state.
   */
  ActionState GetState() { return state_.load(); }

  /**
   * Set the state of this action.
   * @param state action state.
   */
  void SetState(ActionState state) { state_.store(state); }

  /**
   * Get action id.
   * @return action id.
   */
  int32_t GetActionId() { return action_id_; }

 private:
  int32_t action_id_;
  std::atomic<ActionState> state_;
};

}  // namespace terrier::common