#pragma once
#include <atomic>

#include "catalog/catalog_defs.h"
#include "common/strong_typedef.h"

namespace terrier::common {

/**
 * An action's id is an internal identifier of the invocation of an action in the system.
 * It is like a transaction id, but we need to make sure that these two types of ids
 * are distinct from each other so that people don't try to mix them.
 *
 * <b>Important: action_id_t != timestamp_t</b>
 */
STRONG_TYPEDEF(action_id_t, uint64_t);

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
  explicit ActionContext(action_id_t action_id) : action_id_(action_id), state_(ActionState::INITIATED) {}

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
  action_id_t GetActionId() const { return action_id_; }

 private:
  action_id_t action_id_;
  std::atomic<ActionState> state_;
};

}  // namespace terrier::common
