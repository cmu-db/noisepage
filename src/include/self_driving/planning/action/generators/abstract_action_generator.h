#pragma once

#include <map>
#include <memory>
#include <vector>

#include "self_driving/planning/action/action_defs.h"

namespace noisepage {

namespace planner {
class AbstractPlanNode;
}  // namespace planner

namespace settings {
class SettingsManager;
}

namespace selfdriving::pilot {

class AbstractAction;

/**
 * Generate self-driving actions
 */
class AbstractActionGenerator {
 public:
  /**
   * Generate actions for the given workload (query plans) and knob settings
   * @param plans The set of query plans to generate index actions for
   * @param settings_manager SettingsManager
   * @param action_map Maps action id to the pointer of the generated action.
   * @param candidate_actions To hold the ids of the candidate actions
   */
  virtual void GenerateActions(const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                               common::ManagedPointer<settings::SettingsManager> settings_manager,
                               std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                               std::vector<action_id_t> *candidate_actions) = 0;

  virtual ~AbstractActionGenerator() = default;
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
