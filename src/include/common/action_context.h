#pragma once
#include <atomic>

#include "catalog/catalog_defs.h"
#include "common/strong_typedef.h"

namespace noisepage::common {

/**
 * An action's id is an internal identifier of the invocation of an action in the system.
 * It is like a transaction id, but we need to make sure that these two types of ids
 * are distinct from each other so that people don't try to mix them.
 *
 * <b>Important: action_id_t != timestamp_t</b>
 */
STRONG_TYPEDEF_HEADER(action_id_t, uint64_t);

/**
 * The lifecycle states of internal actions.
 */
enum class ActionState : uint8_t {
  /** The system has created the action but the invocation has not started. */
  INITIATED,

  /** The action's invocation has begun. */
  IN_PROGRESS,

  /** The action has completed successfully. */
  SUCCESS,

  /** The action has failed and is terminated. */
  FAILURE,

  /** The action is unable to proeceed now and has been deferred for later execution. */
  DEFERRED,
};

/**
 * ActionContext is used to keep track of action state and system behavior.
 * The DBMS creates an action whenever it makes a change to its configuration
 * as part of the self-driving infrastructure.
 * An 'action' is different from a 'transaction' because it is (potentially) changing
 * the DBMS's physical configuration (e.g., adding more GC threads), and thus does not
 * (and in some cases can not) have ACID guarantees. But an action can contain a transaction
 * if it is making a change to the database's physical design (e.g., add/drop index).
 *
 * Because an action does not have any ACID guarantees, it is up to whomever is using them
 * to properly implement the rollback mechanisms needed to revert the changes upon failure.
 * Note that it is fine if other threads observe the intermediate affects of actions before
 * they complete, or even inconsistent affects.
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

}  // namespace noisepage::common
