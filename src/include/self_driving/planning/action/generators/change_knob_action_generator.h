#pragma once

#include <map>
#include <memory>
#include <vector>

#include "self_driving/planning/action/action_defs.h"
#include "self_driving/planning/action/generators/abstract_action_generator.h"

namespace noisepage {

namespace settings {
class SettingsManager;
}

namespace selfdriving::pilot {

class AbstractAction;

/**
 * Generate change knob candidate actions
 */
class ChangeKnobActionGenerator : AbstractActionGenerator {
 public:
  void GenerateActions(const std::vector<std::unique_ptr<planner::AbstractPlanNode>> &plans,
                       common::ManagedPointer<settings::SettingsManager> settings_manager,
                       std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                       std::vector<action_id_t> *candidate_actions) override;

 private:
  template <class T>
  static void GenerateActionForType(common::ManagedPointer<settings::SettingsManager> settings_manager,
                                    std::map<action_id_t, std::unique_ptr<AbstractAction>> *action_map,
                                    std::vector<action_id_t> *candidate_actions);
};

}  // namespace selfdriving::pilot
}  // namespace noisepage
