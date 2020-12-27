#pragma once

#include "common/hash_defs.h"
#include "common/resource_tracker.h"
#include "self_driving/pilot/action/action_defs.h"

namespace noisepage::selfdriving::pilot {

/**
 * The abstract class for self-driving actions
 */
class AbstractAction {
  /**
   * Constructor for the base AbstractAction.
   * @param family The family that this action belongs to
   */
  explicit AbstractAction(ActionFamily family) : family_(family), id_(action_id_counter_++){};

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
  action_id_t GetActionID() { return id_; }

  /** @return This action's family */
  ActionFamily GetActionFamily() { return action_family_; }

 private:
  static action_id_t action_id_counter_{0};

  common::ResourceTracker::Metrics estimated_metrics_{};

  ActionFamily action_family_;

  /** ID is unique for an action among on planning process (one MCTS) */
  action_id_t id_;
};

}  // namespace noisepage::selfdriving::pilot
