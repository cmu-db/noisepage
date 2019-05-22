#pragma once
#include <atomic>

#include "catalog/catalog_defs.h"

namespace terrier::common {

/**
 * All possible states of action.
 */
enum class ActionState : uint8_t {
  INITIATED,
  IN_PROGRESS,
  SUCCESS,
  FAILURE,
  DEFERRED,
};

/**
 * ActionContext is used to keep track of action state and system behavior.
 */
class ActionContext {
 public:
  ActionContext(const ActionContext &) = delete;

  /**
   * Constructor of ActionContext.
   * @param action_id id of this action.
   */
  explicit ActionContext(int32_t action_id) : action_id_(action_id), state_(ActionState::INITIATED) {}

  /**
   * Get the state of this action.
   * @return action state.
   */
  ActionState GetState() const { return state_.load(); }

  /**
   * Set the state of this action.
   * @param state action state.
   */
  void SetState(ActionState state) { state_.store(state); }

  /**
   * Get action id.
   * @return action id.
   */
  catalog::action_oid_t GetActionId() const { return action_id_; }

 private:
  catalog::action_oid_t action_id_;
  std::atomic<ActionState> state_;
};

}  // namespace terrier::common
