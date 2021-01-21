#pragma once

#include <string>
#include <vector>

#include "common/resource_tracker.h"
#include "self_driving/pilot/action/action_defs.h"

namespace noisepage::selfdriving::pilot {

/**
 * The abstract class for self-driving actions
 */
class AbstractAction {
 public:
  /**
   * Constructor for the base AbstractAction.
   * @param family The family that this action belongs to
   */
  explicit AbstractAction(ActionType family) : action_family_(family), id_(action_id_counter++) {}

  virtual ~AbstractAction() = default;

  /**
   * Set the estimated runtime metrics for this action
   * @param estimated_metrics The metrics to set to
   */
  void SetEstimatedMetrics(const common::ResourceTracker::Metrics &estimated_metrics) {
    estimated_metrics_ = estimated_metrics;
  }

  /** @return The estimated runtime metrics for this action */
  const common::ResourceTracker::Metrics &GetActualMetrics() { return estimated_metrics_; }

  /** @return This action's ID */
  action_id_t GetActionID() const { return id_; }

  /** @return This action's family */
  ActionType GetActionFamily() const { return action_family_; }

  /**
   * Add an equivalent action
   * @param id Action ID
   */
  void AddEquivalentAction(action_id_t id) { equivalent_action_ids_.emplace_back(id); }

  /**
   * Get the equivalent action ids
   * @return Action ID vector
   */
  const std::vector<action_id_t> &GetEquivalentActions() const { return equivalent_action_ids_; }

  /**
   * Add a reverse action
   * @param id Action ID
   */
  void AddReverseAction(action_id_t id) { reverse_action_ids_.emplace_back(id); }

  /**
   * Get the reverse action ids
   * @return Action ID vector
   */
  const std::vector<action_id_t> &GetReverseActions() const { return reverse_action_ids_; }

  /**
   * Get the SQL command to apply the action
   * @return Action SQL command
   */
  virtual const std::string &GetSQLCommand() { return sql_command_; }

  /**
   * Check whether the action is valid to apply.
   * Possible scenarios that the action is invalid to apply: the knob setting is out of the valid range, the index
   * requires more memory than available in the system, etc.
   * TODO(lin): add the available memory as input param
   * @return true if the action is valid to apply, false otherwise
   */
  virtual bool IsValid() { return true; }

 protected:
  std::string sql_command_;  ///< The SQL commaned used to apply the action

 private:
  static action_id_t action_id_counter;

  common::ResourceTracker::Metrics estimated_metrics_{};

  ActionType action_family_;

  /** ID is unique for an action among on planning process (one MCTS) */
  action_id_t id_;

  std::vector<action_id_t> equivalent_action_ids_;
  std::vector<action_id_t> reverse_action_ids_;
};

}  // namespace noisepage::selfdriving::pilot
